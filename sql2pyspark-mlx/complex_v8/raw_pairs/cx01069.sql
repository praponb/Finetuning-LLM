WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at,
        closed_at + INTERVAL 15 MINUTES AS closed_at_plus_15m,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM tickets
    WHERE channel <> 'email'
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.requester_id <= 18
              AND t.closed_at < TIMESTAMP '2025-02-20 15:00:00'
        )
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_agent_id_closed_at_plus_15m, 3) AS closed_at_cume_dist,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id, csat), 2) AS csat_vs_agent_id_csat_avg,
        MAX(csat) OVER (PARTITION BY TO_DATE(closed_at) ORDER BY minutes_closed_at_to_opened_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_max_2_prior,
        FIRST_VALUE(csat) OVER (PARTITION BY agent_id, MONTH(closed_at_plus_15m) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        NTILE(5) OVER (PARTITION BY agent_id, MONTH(closed_at_plus_15m) ORDER BY closed_at, id) AS closed_at_quintile
    FROM without_matches
    WINDOW w_agent_id_closed_at_plus_15m AS (PARTITION BY agent_id, MONTH(closed_at_plus_15m) ORDER BY closed_at)
)
-- Subtotals with CUBE
SELECT
    csat,
    agent_id,
    GROUPING_ID() AS gid,
    MIN(closed_at_cume_dist) AS min_closed_at_cume_dist,
    COUNT(*) AS cnt
FROM ranked
GROUP BY CUBE(csat, agent_id)
