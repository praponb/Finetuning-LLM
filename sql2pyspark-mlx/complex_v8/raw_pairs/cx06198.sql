WITH joined_ranges AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_3d,
        SUM(b.csat) AS prior_csat_3d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 3 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY prior_csat_3d ASC, id) AS prior_csat_3d_seq,
        SIZE(COLLECT_SET(agent_id) OVER w_prior_rows_3d) AS distinct_agent_id_in_prior_rows_3d,
        ROUND(AVG(prior_rows_3d) OVER w_prior_rows_3d, 2) AS prior_rows_3d_avg_prior_rows_3d
    FROM joined_ranges
    WINDOW w_prior_rows_3d AS (PARTITION BY prior_rows_3d)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(10) OVER (ORDER BY distinct_agent_id_in_prior_rows_3d DESC NULLS FIRST, id) AS distinct_agent_id_in_prior_rows_3d_decile,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY prior_rows_3d ORDER BY prior_rows_3d, id))) / 60, 1) AS minutes_since_prev_opened_at,
        SUM(prior_csat_3d_seq) OVER (PARTITION BY prior_rows_3d ORDER BY prior_rows_3d, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS prior_csat_3d_seq_sum_2,
        ROUND(AVG(prior_csat_3d_seq) OVER (PARTITION BY distinct_agent_id_in_prior_rows_3d, agent_id), 2) AS distinct_agent_id_in_prior_rows_3d_agent_id_avg_,
        DENSE_RANK() OVER (PARTITION BY prior_rows_3d ORDER BY prior_rows_3d) AS prior_rows_3d_dense_rank,
        LAST_VALUE(prior_rows_3d) OVER (ORDER BY distinct_agent_id_in_prior_rows_3d DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prior_rows_3d
    FROM ranked
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at >= TIMESTAMP '2025-01-13 14:00:00'
              AND t.event_type <> 'assigned'
        )
)
SELECT
    id,
    opened_at,
    prior_rows_3d,
    prior_csat_3d,
    prior_csat_3d_seq,
    distinct_agent_id_in_prior_rows_3d,
    prior_rows_3d_avg_prior_rows_3d,
    distinct_agent_id_in_prior_rows_3d_decile,
    prior_csat_3d_seq_sum_2,
    distinct_agent_id_in_prior_rows_3d_agent_id_avg_,
    prior_rows_3d_dense_rank,
    last_prior_rows_3d,
    CASE WHEN prior_csat_3d >= 19 THEN 'HIGH' WHEN prior_csat_3d >= 6 THEN 'MEDIUM' ELSE 'LOW' END AS prior_csat_3d_tier
FROM filtered
