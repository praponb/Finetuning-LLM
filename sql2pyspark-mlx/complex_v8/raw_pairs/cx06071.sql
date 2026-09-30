WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE actor NOT IN ('customer', 'agent')
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor_ref,
        RANK() OVER w_ticket_id_actor_by_event_at AS event_at_rank,
        ROUND(PERCENT_RANK() OVER w_event_type_by_event_at, 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at DESC, id) AS event_at_rank_2,
        FIRST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS rn,
        ROUND(PERCENT_RANK() OVER w_event_type_by_event_at_ties, 3) AS event_at_percent_rank_2,
        ROW_NUMBER() OVER w_event_type_by_event_at AS seq_no,
        LAG(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS prev_event_at,
        NTILE(4) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS event_at_quartile,
        DENSE_RANK() OVER w_event_type_by_event_at_2 AS event_at_dense_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM prepared_events
    WINDOW w_ticket_id_actor_by_event_at AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at, id), w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at DESC), w_event_type_by_event_at_2 AS (PARTITION BY event_type ORDER BY event_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE event_at_dense_rank = 1
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor_ref,
        event_at_rank,
        event_at_rank_2,
        first_event_at,
        rn,
        prev_event_at,
        event_at_quartile,
        event_at_dense_rank,
        minutes_since_prev_event_at,
        LAST_VALUE(seq_no) OVER (PARTITION BY DATE_TRUNC('MONTH', second_event_at) ORDER BY event_at_quartile, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_seq_no,
        MAX(minutes_since_prev_event_at) OVER (PARTITION BY DATE_TRUNC('MONTH', second_event_at) ORDER BY event_at_quartile, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS minutes_since_prev_event_at_max_2_prior,
        ROW_NUMBER() OVER (ORDER BY rn, id) AS rn_seq,
        DENSE_RANK() OVER (ORDER BY rn) AS rn_dense_rank,
        ROUND(AVG(event_at_rank) OVER w_prev_event_at_to_date_seq_no, 2) AS prev_event_at_to_date_seq_no_avg_event_at_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_at_rank_2, event_at_rank ORDER BY event_at_percent_rank_2 DESC), 3) AS event_at_percent_rank_2_percent_rank
    FROM top_rows
    WINDOW w_prev_event_at_to_date_seq_no AS (PARTITION BY TO_DATE(prev_event_at), seq_no)
),
summary AS (
    -- Group-level totals
    SELECT
        rn_dense_rank,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        SUM(CASE WHEN event_type = 'replied' THEN 1 ELSE 0 END) AS n_event_type_replied,
        MIN(rn) AS min_rn,
        MAX(event_at) AS last_event_at
    FROM metrics
    WHERE rn_seq < 9
    GROUP BY rn_dense_rank
    HAVING MAX(event_at_percent_rank_2_percent_rank) > 0.3
),
metrics_2 AS (
    -- Previous / next values per key
    SELECT
        rn_dense_rank,
        event_type_list,
        ROUND(CUME_DIST() OVER (PARTITION BY min_rn, rn_dense_rank ORDER BY n_event_type_replied DESC), 3) AS n_event_type_replied_cume_dist,
        NTILE(10) OVER (PARTITION BY min_rn, rn_dense_rank ORDER BY n_event_type_replied DESC, rn_dense_rank) AS n_event_type_replied_decile
    FROM summary
)
SELECT
    rn_dense_rank,
    event_type_list,
    FLOOR(rn_dense_rank / 2) * 2 AS rn_dense_rank_bucket,
    ABS(rn_dense_rank - 6) AS rn_dense_rank_distance
FROM metrics_2
ORDER BY rn_dense_rank_distance NULLS LAST, rn_dense_rank DESC
