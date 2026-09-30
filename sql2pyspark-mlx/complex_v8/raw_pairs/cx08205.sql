WITH valid_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        SUBSTRING(TRIM(tier), 1, 3) AS tier_prefix,
        LOWER(TRIM(team)) AS team_clean
    FROM support_agents
    WHERE hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-11-09'
      AND team IN ('VIP', 'Tech', 'Billing')
),
deduped AS (
    -- Keep the earliest row per team
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        tier_prefix,
        team_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team ORDER BY hired_on, id) AS rn
        FROM valid_agents
    ) ranked
    WHERE rn = 1
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM deduped s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
)
SELECT
    ticket_count,
    COALESCE(team, 'ALL') AS team,
    COALESCE(tier, 'ALL') AS tier,
    GROUPING_ID() AS grouping_level,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(ticket_count, 0.5) AS median_ticket_count,
    ARRAY_SORT(COLLECT_SET(team)) AS team_list
FROM summarized_rows
GROUP BY ROLLUP(ticket_count, team, tier)
ORDER BY ticket_count NULLS LAST, team NULLS LAST, tier NULLS LAST
