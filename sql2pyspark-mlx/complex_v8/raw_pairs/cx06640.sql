WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        CONCAT_WS('|', priority, channel) AS priority_channel_label
    FROM tickets
    WHERE SIZE(tags) >= 2
),
qualified AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM scoped_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND ARRAY_CONTAINS(t.tags, 'login')
              AND t.requester_id < 19
        )
      AND s.channel IN ('web', 'phone', 'chat')
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        su.team AS agent_team
    FROM screened s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(csat) OVER w_agent_team, 2) AS agent_team_avg_csat,
        LEAD(csat, 3) OVER w_agent_team_priority_by_close AS next3_csat,
        LAG(csat, 2) OVER (PARTITION BY agent_team, priority ORDER BY closed_at ASC, id) AS prev2_csat,
        ROUND(AVG(csat) OVER (PARTITION BY agent_team, priority ORDER BY closed_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS csat_avg_3
    FROM with_parent
    WINDOW w_agent_team AS (PARTITION BY agent_team), w_agent_team_priority_by_close AS (PARTITION BY agent_team, priority ORDER BY closed_at ASC, id)
),
per_group AS (
    -- Summarise per group
    SELECT
        priority,
        next3_csat,
        prev2_csat,
        ROUND(100 * COUNT(CASE WHEN agent_team NOT IN ('VIP', 'Onboarding') THEN 1 END) / COUNT(*), 1) AS pct_agent_team_not_in_vip_onboar,
        COUNT(*) FILTER (WHERE channel IN ('phone', 'web')) AS n_channel_in_phone_web
    FROM with_windows
    GROUP BY 1, 2, 3
)
-- Native SQL UNPIVOT (wide to long)
SELECT priority, next3_csat, metric, amount
FROM (
    SELECT priority, next3_csat, prev2_csat, n_channel_in_phone_web
    FROM per_group
) src
UNPIVOT (
    amount FOR metric IN (prev2_csat, n_channel_in_phone_web)
) unp
