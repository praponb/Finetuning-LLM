WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        CONCAT_WS('-', priority, channel) AS priority_channel_label,
        TO_DATE(opened_at) AS opened_at_date
    FROM tickets
),
with_details AS (
    -- Join support_agents
    SELECT s.*, su.team AS agent_team, su.tier AS agent_tier
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(agent_tier)) AS agent_tier_list
    FROM with_details
    WHERE requester_id BETWEEN 4 AND 18
    GROUP BY DATE_TRUNC('MONTH', closed_at), DATE_TRUNC('MONTH', opened_at)
),
tagged AS (
    -- Label and score rows
    SELECT
        closed_at_month,
        opened_at_month,
        row_count,
        agent_tier_list,
        CASE WHEN row_count >= 9 THEN 'A' WHEN row_count >= 5 THEN 'B' ELSE 'C' END AS row_count_tier,
        YEAR(closed_at_month) AS closed_at_month_year
    FROM stats
)
SELECT closed_at_month, opened_at_month, field, amount
FROM (
    SELECT closed_at_month, opened_at_month, row_count, closed_at_month_year
    FROM tagged
) src
UNPIVOT INCLUDE NULLS (
    amount FOR field IN (row_count, closed_at_month_year)
) unp
ORDER BY closed_at_month, opened_at_month, field
