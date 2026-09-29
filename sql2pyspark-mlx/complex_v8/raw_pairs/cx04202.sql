WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        ABS(requester_id - 17) AS requester_id_distance
    FROM tickets
    WHERE requester_id >= 7
),
combined_details AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.agent_name,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND ARRAY_CONTAINS(t.tags, 'bug')
        )
      AND s.agent_id >= 8
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label,
        SIZE(tags) AS tags_count,
        INITCAP(TRIM(agent_name)) AS agent_name_clean
    FROM with_activity
    WHERE agent_team NOT IN ('Onboarding', 'Billing')
),
agent_tier_stats AS (
    -- Max csat per agent_tier
    SELECT agent_tier, MAX(csat) AS max_csat
    FROM tagged
    GROUP BY agent_tier
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM tagged s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.csat = g.max_csat
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY csat, agent_id ORDER BY requester_id_distance, id) AS requester_id_distance_dense_rank,
        COUNT(*) OVER w_opened_at_month_agent_team AS rows_in_opened_at_month_agent_team,
        FIRST_VALUE(csat) OVER (ORDER BY opened_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat
    FROM vs_group
    WINDOW w_opened_at_month_agent_team AS (PARTITION BY MONTH(opened_at), agent_team)
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.agent_name,
        s.agent_team,
        s.agent_tier,
        s.tags_count,
        s.agent_name_clean,
        s.max_csat,
        s.csat_minus_max,
        s.requester_id_distance_dense_rank,
        s.rows_in_opened_at_month_agent_team,
        s.first_csat,
        su.agent_name AS agent_agent_name
    FROM metrics s
    JOIN support_agents su ON su.id = s.agent_id
)
SELECT
    id,
    agent_id,
    opened_at,
    closed_at,
    priority,
    agent_name,
    agent_team,
    agent_tier,
    agent_name_clean,
    max_csat,
    requester_id_distance_dense_rank,
    rows_in_opened_at_month_agent_team,
    agent_agent_name,
    DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_label
FROM joined
ORDER BY opened_at
