WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.agent_id,
        a.opened_at,
        COUNT(b.id) AS prior_rows_1d,
        SUM(b.csat) AS prior_csat_1d
    FROM tickets a
    LEFT JOIN tickets b
      ON b.agent_id = a.agent_id
     AND b.opened_at < a.opened_at
     AND b.opened_at >= a.opened_at - INTERVAL 1 DAYS
    GROUP BY a.id, a.agent_id, a.opened_at
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(opened_at, 2) OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id) AS next2_opened_at,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY prior_rows_1d), 3) AS prior_rows_1d_cume_dist,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY prior_rows_1d)) AS distinct_agent_id_in_prior_rows_1d
    FROM covered
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM with_trends
    WHERE prior_rows_1d_cume_dist < (SELECT MAX(prior_rows_1d_cume_dist) FROM with_trends)
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS first_event_at
    FROM qualified s
)
SELECT id, metric_name, amount
FROM (
    SELECT id, prior_csat_1d, prior_rows_1d, distinct_agent_id_in_prior_rows_1d
    FROM with_counts
) src
UNPIVOT INCLUDE NULLS (
    amount FOR metric_name IN (prior_csat_1d AS m_prior_csat_1d, prior_rows_1d AS m_prior_rows_1d, distinct_agent_id_in_prior_rows_1d AS m_distinct_agent_id_in_prior_rows_1d)
) unp
