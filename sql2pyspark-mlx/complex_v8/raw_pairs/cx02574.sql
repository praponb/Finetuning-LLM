WITH recent AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        CASE WHEN team IN ('Billing', 'Onboarding') THEN 'group_a' WHEN team = 'Tech' THEN 'group_b' ELSE 'other' END AS team_group,
        LENGTH(TRIM(agent_name)) AS agent_name_len,
        SUBSTRING(TRIM(team), 1, 3) AS team_prefix
    FROM support_agents
    WHERE team NOT IN ('Billing', 'Onboarding')
      AND hired_on >= DATE '2024-05-02'
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM recent
    WHERE agent_name_len < (SELECT MAX(agent_name_len) FROM recent)
),
screened_2 AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
        )
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.requester_id), 2) FROM tickets t WHERE t.agent_id = s.id) AS avg_ticket_requester_id
    FROM screened_2 s
)
SELECT
    COALESCE(team_group, 'ALL') AS team_group,
    COALESCE(team, 'ALL') AS team,
    GROUPING(team_group) AS is_total_team_group,
    MAX(avg_ticket_requester_id) AS max_avg_ticket_requester_id,
    SUM(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'H%' THEN agent_name_len ELSE 0 END) AS agent_name_len_upper_trim_agent_name_like_h,
    COUNT(*) AS n_rows
FROM with_stats
GROUP BY CUBE(team_group, team)
ORDER BY team_group NULLS LAST, team NULLS LAST
