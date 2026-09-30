WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at,
        UPPER(TRIM(channel)) AS channel_clean,
        CONCAT_WS(': ', priority, channel) AS priority_channel_label
    FROM tickets
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
        COALESCE(days_since_opened_at, 0) AS days_since_opened_at_filled,
        QUARTER(opened_at) AS opened_at_quarter
    FROM prepared_tickets
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM tagged s
    WHERE s.agent_id IN (
            SELECT t.agent_id
            FROM tickets t
            WHERE SIZE(t.tags) >= 1
              AND HOUR(t.closed_at) BETWEEN 6 AND 11
        )
),
agent_id_stats AS (
    -- Avg days_since_opened_at_filled per agent_id
    SELECT agent_id, AVG(days_since_opened_at_filled) AS avg_days_since_opened_at_filled
    FROM screened
    GROUP BY agent_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_opened_at_filled,
        ROUND(s.days_since_opened_at_filled - g.avg_days_since_opened_at_filled, 2) AS days_since_opened_at_filled_minus_avg
    FROM screened s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.days_since_opened_at_filled > g.avg_days_since_opened_at_filled
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, metric_value
FROM (
    SELECT id, avg_days_since_opened_at_filled, days_since_opened_at_filled_minus_avg
    FROM outliers
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR metric_name IN (avg_days_since_opened_at_filled, days_since_opened_at_filled_minus_avg)
) unp
ORDER BY id, metric_name
