WITH valid_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATE_FORMAT(hired_on, 'yyyy-MM-dd') AS hired_on_label,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 5
      AND team = 'Tech'
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, COUNT(*) AS ticket_count
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        COALESCE(t.ticket_count, 0) AS ticket_count
    FROM valid_agents s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
filtered AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND t.closed_at >= TIMESTAMP '2025-01-25 11:00:00'
              AND ARRAY_CONTAINS(t.tags, 'login')
        )
),
tier_stats AS (
    -- Avg ticket_count per tier
    SELECT tier, AVG(ticket_count) AS avg_ticket_count
    FROM filtered
    GROUP BY tier
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ticket_count,
        ROUND(s.ticket_count - g.avg_ticket_count, 2) AS ticket_count_minus_avg
    FROM filtered s
    JOIN tier_stats g ON g.tier = s.tier
    WHERE s.ticket_count > g.avg_ticket_count
)
-- Subtotals with CUBE
SELECT
    team,
    tier_or_unknown,
    COUNT(*) FILTER (WHERE team IN ('VIP', 'Tech')) AS n_team_in_vip_tech,
    MAX(avg_ticket_count) AS max_avg_ticket_count
FROM outliers
GROUP BY CUBE(team, tier_or_unknown)
ORDER BY team NULLS LAST, tier_or_unknown NULLS LAST
