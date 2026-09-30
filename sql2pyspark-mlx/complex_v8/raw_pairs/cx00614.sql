WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        YEAR(event_at) AS event_at_year,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE actor = 'agent'
      AND event_type = 'created'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_all_by_event_at_year_ties AS event_at_year_rank,
        ROUND(AVG(event_at_year) OVER (PARTITION BY event_at_year, actor ORDER BY event_at, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_at_year_avg_3
    FROM recent
    WINDOW w_all_by_event_at_year_ties AS (ORDER BY event_at_year)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        event_at_year,
        event_at_label,
        event_at_year_rank,
        DENSE_RANK() OVER w_actor_event_type_by_event_at AS event_at_year_rank_dense_rank,
        SUM(event_at_year) OVER (PARTITION BY actor, event_type ORDER BY event_at_year_rank NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS event_at_year_sum_7,
        MAX(event_at_year_rank) OVER (PARTITION BY actor ORDER BY event_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_at_year_rank_max_3,
        ROUND(AVG(event_at_year_rank) OVER (PARTITION BY YEAR(event_at), event_at_year), 2) AS event_at_year_event_at_year_avg_event_at_year_ra,
        RANK() OVER (PARTITION BY actor ORDER BY event_at ASC) AS event_at_rank,
        SIZE(COLLECT_SET(actor) OVER w_event_at_year_event_at_year) AS distinct_actor_in_event_at_year_event_at_year
    FROM ranked
    WINDOW w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at_year_rank NULLS LAST), w_event_at_year_event_at_year AS (PARTITION BY YEAR(event_at), event_at_year)
),
stats AS (
    -- Group-level totals
    SELECT
        event_at_year_rank_dense_rank,
        SUM(event_at_year) AS total_event_at_year,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN actor = 'customer' THEN 1 ELSE 0 END) AS n_actor_customer,
        ROUND(SUM(event_at_year) / NULLIF(SUM(event_at_year_rank), 0), 3) AS event_at_year_to_event_at_year_rank_ratio,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids
    FROM with_windows
    GROUP BY event_at_year_rank_dense_rank
    HAVING AVG(event_at_year_rank_max_3) > 10
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE n_rows > (SELECT MIN(n_rows) FROM stats)
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_ticket_ids,
    event_at_year_rank_dense_rank,
    MIN(total_event_at_year) AS min_total_event_at_year,
    ROUND(STDDEV(event_at_year_rank_dense_rank), 2) AS stddev_event_at_year_rank_dense_rank,
    MIN(event_at_year_to_event_at_year_rank_ratio) AS min_event_at_year_to_event_at_year_rank_ratio
FROM without_matches
GROUP BY GROUPING SETS ((distinct_ticket_ids, event_at_year_rank_dense_rank), (distinct_ticket_ids))
ORDER BY distinct_ticket_ids NULLS LAST, event_at_year_rank_dense_rank NULLS LAST
