WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE (event_type = 'replied' OR actor = 'customer')
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        event_at_year,
        FIRST_VALUE(event_at_year) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at_year,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_cume_dist,
        ROUND(AVG(event_at_year) OVER (ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS event_at_year_avg_7,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DENSE_RANK() OVER (PARTITION BY actor, event_type ORDER BY event_at, id) AS event_at_dense_rank,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id)) AS distinct_actor_in_ticket_id,
        ROUND(CUME_DIST() OVER (ORDER BY event_at DESC NULLS FIRST), 3) AS event_at_cume_dist_2,
        ROUND(event_at_year / NULLIF(SUM(event_at_year) OVER (PARTITION BY event_type), 0), 4) AS event_at_year_share_of_event_type,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC), 3) AS event_at_percent_rank
    FROM cleaned
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        event_type,
        event_at_year,
        first_event_at_year,
        event_at_cume_dist,
        event_at_year_avg_7,
        last_known_event_at,
        event_at_dense_rank,
        distinct_actor_in_ticket_id,
        event_at_year_share_of_event_type,
        event_at_percent_rank,
        ROUND(distinct_actor_in_ticket_id - AVG(distinct_actor_in_ticket_id) OVER (PARTITION BY first_event_at_year), 2) AS distinct_actor_in_ticket_id_vs_first_event_at_ye,
        LAG(event_at_percent_rank) OVER (PARTITION BY MONTH(event_at) ORDER BY last_known_event_at, id) AS prev_event_at_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY MONTH(event_at) ORDER BY last_known_event_at, id), 3) AS last_known_event_at_cume_dist,
        LAST_VALUE(event_at_cume_dist) OVER (PARTITION BY MONTH(event_at) ORDER BY last_known_event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at_cume_dist,
        DENSE_RANK() OVER (PARTITION BY MONTH(event_at) ORDER BY last_known_event_at, id) AS last_known_event_at_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY MONTH(event_at) ORDER BY last_known_event_at, id) AS seq_no,
        LAG(event_at_cume_dist_2) OVER (PARTITION BY MONTH(event_at) ORDER BY last_known_event_at, id) AS prev_event_at_cume_dist_2
    FROM ranked
),
stats AS (
    -- Group-level totals
    SELECT
        distinct_actor_in_ticket_id,
        event_at_dense_rank,
        last_known_event_at_dense_rank,
        SUM(event_at_year) AS total_event_at_year,
        COUNT(*) AS n_rows,
        SUM(last_known_event_at_cume_dist) AS total_last_known_event_at_cume_dist,
        SUM(CASE WHEN event_type NOT IN ('reopened', 'created') THEN 1 ELSE 0 END) AS n_event_type_not_in_reopened_c,
        COUNT(DISTINCT event_type) AS distinct_event_types,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
    FROM sequenced
    GROUP BY distinct_actor_in_ticket_id, event_at_dense_rank, last_known_event_at_dense_rank
),
ranked_2 AS (
    -- Previous / next values per key
    SELECT
        distinct_actor_in_ticket_id,
        event_at_dense_rank,
        last_known_event_at_dense_rank,
        total_event_at_year,
        n_rows,
        total_last_known_event_at_cume_dist,
        n_event_type_not_in_reopened_c,
        distinct_event_types,
        ROUND(n_rows - AVG(n_rows) OVER w_n_rows, 2) AS n_rows_vs_n_rows_avg,
        ROUND(AVG(event_at_dense_rank) OVER (PARTITION BY n_rows), 2) AS n_rows_avg_event_at_dense_rank
    FROM stats
    WINDOW w_n_rows AS (PARTITION BY n_rows)
)
-- Final projection
SELECT
    distinct_actor_in_ticket_id,
    event_at_dense_rank,
    total_event_at_year,
    n_rows,
    n_event_type_not_in_reopened_c,
    distinct_event_types,
    n_rows_avg_event_at_dense_rank,
    FLOOR(n_rows_vs_n_rows_avg / 5) * 5 AS n_rows_vs_n_rows_avg_bucket,
    n_rows_vs_n_rows_avg - distinct_event_types AS n_rows_vs_n_rows_avg_minus_distinct_event_types
FROM ranked_2
WHERE total_last_known_event_at_cume_dist < 3.6
ORDER BY distinct_actor_in_ticket_id DESC, n_event_type_not_in_reopened_c DESC
