WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        ABS(csat - 4) AS csat_distance,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(closed_at)) / 60, 1) AS minutes_closed_at_to_opened_at
    FROM tickets
    WHERE HOUR(closed_at) BETWEEN 9 AND 15
      AND opened_at < TIMESTAMP '2025-02-06 15:00:00'
),
latest AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        agent_id,
        closed_at,
        channel,
        csat,
        csat_distance,
        minutes_closed_at_to_opened_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY closed_at DESC, id DESC) AS row_num
        FROM base
    ) ranked
    WHERE row_num = 1
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM latest
    WHERE csat_distance < (SELECT MAX(csat_distance) FROM latest)
),
screened AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM without_matches s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND MONTH(t.opened_at) IN (1, 3)
        )
)
-- Subtotals with GROUPING SETS
SELECT
    channel,
    agent_id,
    csat,
    GROUPING_ID() AS gid,
    MAX(csat_distance) AS max_csat_distance,
    PERCENTILE_APPROX(csat_distance, 0.5) AS median_csat_distance
FROM screened
GROUP BY GROUPING SETS ((channel, agent_id, csat), (channel), ())
