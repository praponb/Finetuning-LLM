WITH valid_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE priority <> 'P2'
),
with_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        s.days_since_opened_at,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM valid_tickets s
    JOIN support_agents su ON su.id = s.agent_id AND su.hired_on >= DATE '2024-05-02'
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', agent_hired_on) AS agent_hired_on_month,
        SUM(days_since_opened_at) AS total_days_since_opened_at,
        MAX(agent_hired_on) AS last_agent_hired_on,
        MAX(days_since_opened_at) AS max_days_since_opened_at,
        ROUND(SUM(csat) / NULLIF(SUM(days_since_opened_at), 0), 3) AS csat_to_days_since_opened_at_ratio,
        COUNT(*) AS row_count
    FROM with_details
    GROUP BY 1
    HAVING AVG(csat) > 2
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM grouped
    WHERE total_days_since_opened_at < (SELECT MAX(total_days_since_opened_at) FROM grouped)
),
labeled AS (
    -- Derive helper columns
    SELECT
        agent_hired_on_month,
        total_days_since_opened_at,
        last_agent_hired_on,
        max_days_since_opened_at,
        csat_to_days_since_opened_at_ratio,
        row_count,
        max_days_since_opened_at - row_count AS max_days_since_opened_at_minus_row_count
    FROM with_activity
    WHERE TO_DATE(agent_hired_on_month) < DATE '2025-01-25'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        agent_hired_on_month,
        max_days_since_opened_at,
        row_count,
        COUNT(*) OVER w_last_agent_hired_on_month_ro AS rows_in_last_agent_hired_on_month_row_count,
        ROUND(total_days_since_opened_at - AVG(total_days_since_opened_at) OVER w_last_agent_hired_on_month_ro, 2) AS total_days_since_opened_at_vs_last_agent_hired_o
    FROM labeled
    WINDOW w_last_agent_hired_on_month_ro AS (PARTITION BY DATE_TRUNC('MONTH', last_agent_hired_on), row_count)
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM ranked
    WHERE rows_in_last_agent_hired_on_month_row_count > (SELECT AVG(rows_in_last_agent_hired_on_month_row_count) FROM ranked)
)
-- Final projection
SELECT
    agent_hired_on_month,
    max_days_since_opened_at,
    row_count,
    rows_in_last_agent_hired_on_month_row_count,
    total_days_since_opened_at_vs_last_agent_hired_o,
    ROUND(max_days_since_opened_at / NULLIF(row_count, 0), 3) AS max_days_since_opened_at_per_row_count
FROM qualified
ORDER BY max_days_since_opened_at, max_days_since_opened_at_per_row_count
