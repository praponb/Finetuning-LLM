WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        opened_at + INTERVAL 2 HOURS AS opened_at_plus_2h,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label
    FROM tickets
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered_tickets
    WHERE csat > (SELECT AVG(csat) FROM filtered_tickets)
),
channel_stats AS (
    -- Avg csat per channel
    SELECT channel, AVG(csat) AS avg_csat
    FROM without_matches
    GROUP BY channel
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM without_matches s
    JOIN channel_stats g ON g.channel = s.channel
    WHERE s.csat > g.avg_csat
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, su.agent_name
    FROM outliers s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    priority,
    csat,
    PERCENTILE_APPROX(avg_csat, 0.5) AS median_avg_csat,
    SUM(csat_minus_avg) FILTER (WHERE UPPER(TRIM(agent_name)) LIKE 'H%') AS csat_minus_avg_upper_trim_agent_name_like_h
FROM combined_details
GROUP BY GROUPING SETS ((agent_id, priority, csat), (agent_id), ())
