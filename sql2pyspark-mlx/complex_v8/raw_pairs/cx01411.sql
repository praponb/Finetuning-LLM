WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier
    FROM support_agents
    WHERE (team IN ('Tech', 'Onboarding') OR tier = 'T2')
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS last_ticket_opened_at,
        (SELECT MIN(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS first_ticket_closed_at,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.id) AS ticket_count
    FROM valid s
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        last_ticket_opened_at,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        DATEDIFF(DATE '2025-12-31', last_ticket_opened_at) AS days_since_last_ticket_opened_at,
        DATE_FORMAT(first_ticket_closed_at, 'yyyy-MM') AS first_ticket_closed_at_label
    FROM summarized_rows
)
SELECT
    team,
    team_or_unknown,
    tier,
    SUM(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'G%' THEN 1 ELSE 0 END) AS n_upper_trim_agent_name_like_g,
    COUNT(DISTINCT team) AS distinct_teams
FROM tagged
GROUP BY ROLLUP(team, team_or_unknown, tier)
ORDER BY team NULLS LAST, team_or_unknown NULLS LAST, tier NULLS LAST
