WITH prepared AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year,
        CONCAT_WS('-', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE requester_id > 7
      AND priority <> 'P4'
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        s.closed_at_year,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM joined s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at < TIMESTAMP '2025-02-18 12:00:00'
              AND t.actor <> 'system'
        )
),
priority_stats AS (
    -- Median csat per priority
    SELECT priority, PERCENTILE_APPROX(csat, 0.5) AS median_csat
    FROM with_activity
    GROUP BY priority
),
compared AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.median_csat, ROUND(s.csat - g.median_csat, 2) AS csat_minus_median
    FROM with_activity s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.csat >= g.median_csat
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM compared s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
        )
),
agent_id_stats AS (
    -- Avg csat per agent_id
    SELECT agent_id, AVG(csat) AS avg_csat
    FROM filtered
    GROUP BY agent_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.avg_csat, ROUND(s.csat - g.avg_csat, 2) AS csat_minus_avg
    FROM filtered s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.csat > g.avg_csat
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        agent_id,
        csat,
        closed_at_year,
        csat_minus_median,
        avg_csat,
        csat_minus_avg,
        LAST_VALUE(avg_csat) IGNORE NULLS OVER (PARTITION BY agent_team, csat ORDER BY csat_minus_median, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_avg_csat,
        NTILE(4) OVER (PARTITION BY agent_team, csat ORDER BY csat_minus_median, id) AS csat_minus_median_quartile,
        csat_minus_median - LAG(csat_minus_median) OVER w_priority_by_avg_csat AS csat_minus_median_change,
        COUNT(*) OVER w_agent_hired_on_year_agent_te AS rows_in_agent_hired_on_year_agent_team
    FROM above_average
    WINDOW w_priority_by_avg_csat AS (PARTITION BY priority ORDER BY avg_csat ASC, id), w_agent_hired_on_year_agent_te AS (PARTITION BY YEAR(agent_hired_on), agent_team)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, value
FROM (
    SELECT id, csat_minus_median_quartile, rows_in_agent_hired_on_year_agent_team
    FROM ranked
) src
UNPIVOT (
    value FOR measure IN (csat_minus_median_quartile, rows_in_agent_hired_on_year_agent_team)
) unp
