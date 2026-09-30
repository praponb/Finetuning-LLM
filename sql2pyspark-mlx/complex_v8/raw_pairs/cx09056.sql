WITH filtered AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        SUBSTRING(TRIM(agent_name), 1, 2) AS agent_name_prefix
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 7
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        team,
        hired_on,
        FIRST_VALUE(hired_on) OVER (PARTITION BY team ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on,
        LAG(hired_on, 2) OVER w_team_by_hired_on AS prev2_hired_on,
        DATEDIFF(hired_on, LAG(hired_on) OVER w_team_by_hired_on_2) AS days_since_prev_hired_on,
        LEAD(hired_on) OVER (PARTITION BY team ORDER BY hired_on, id) AS next_hired_on
    FROM filtered
    WINDOW w_team_by_hired_on AS (PARTITION BY team ORDER BY hired_on, id), w_team_by_hired_on_2 AS (PARTITION BY team ORDER BY hired_on DESC, id)
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND MONTH(t.closed_at) IN (1, 3)
        )
),
deduped AS (
    -- Keep the latest row per team
    SELECT
        id,
        team,
        hired_on,
        first_hired_on,
        prev2_hired_on,
        days_since_prev_hired_on,
        next_hired_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY hired_on DESC, id DESC) AS row_num
        FROM qualified
    ) ranked
    WHERE row_num = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        team,
        DATE_FORMAT(first_hired_on, 'yyyy-MM') AS first_hired_on_ym,
        ROUND(100 * COUNT(CASE WHEN team NOT IN ('Billing', 'VIP') THEN 1 END) / COUNT(*), 1) AS pct_team_not_in_billing_vip,
        ARRAY_SORT(COLLECT_SET(team)) AS team_list,
        ROUND(AVG(days_since_prev_hired_on), 2) AS avg_days_since_prev_hired_on,
        MIN(first_hired_on) AS first_first_hired_on,
        MIN(days_since_prev_hired_on) AS min_days_since_prev_hired_on,
        COUNT(DISTINCT id) AS distinct_ids
    FROM deduped
    GROUP BY team, DATE_FORMAT(first_hired_on, 'yyyy-MM')
)
SELECT
    team,
    first_hired_on_ym,
    pct_team_not_in_billing_vip,
    team_list,
    avg_days_since_prev_hired_on,
    first_first_hired_on,
    min_days_since_prev_hired_on,
    distinct_ids,
    QUARTER(first_first_hired_on) AS first_first_hired_on_quarter,
    CASE WHEN team IN ('VIP', 'Onboarding') THEN 'group_a' WHEN team = 'Billing' THEN 'group_b' ELSE 'other' END AS team_group
FROM grouped
ORDER BY distinct_ids, avg_days_since_prev_hired_on
