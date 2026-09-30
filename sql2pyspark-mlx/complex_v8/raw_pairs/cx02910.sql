WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat
    FROM tickets
    WHERE (agent_id <= 5 OR HOUR(opened_at) BETWEEN 7 AND 12)
),
enriched AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        su.team AS agent_team
    FROM valid s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(AVG(csat) OVER (ORDER BY csat, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS csat_avg_7,
        LAST_VALUE(closed_at) OVER (ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at,
        ROUND(AVG(csat) OVER (PARTITION BY csat, TO_DATE(closed_at)), 2) AS csat_closed_at_to_date_avg_csat,
        MIN(csat) OVER (PARTITION BY csat, TO_DATE(closed_at)) AS csat_closed_at_to_date_min_csat
    FROM enriched
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    csat,
    agent_team,
    csat_avg_7,
    csat_closed_at_to_date_avg_csat,
    csat_closed_at_to_date_min_csat,
    DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at,
    TO_DATE(closed_at) AS closed_at_date
FROM with_windows
