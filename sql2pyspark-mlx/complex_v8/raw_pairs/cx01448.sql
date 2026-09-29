WITH recent_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        YEAR(hired_on) AS hired_on_year
    FROM support_agents
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(t.requester_id), 2) FROM tickets t WHERE t.agent_id = s.id) AS avg_ticket_requester_id,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS last_ticket_closed_at,
        (SELECT MAX(t.csat) FROM tickets t WHERE t.agent_id = s.id) AS max_ticket_csat
    FROM recent_agents s
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM with_stats s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.id
              AND MONTH(t.opened_at) IN (1, 2)
        )
      AND s.hired_on BETWEEN DATE '2024-04-14' AND DATE '2025-03-30'
),
totals AS (
    -- Summarise per group
    SELECT
        tier,
        TO_DATE(last_ticket_closed_at) AS last_ticket_closed_at_day,
        MAX(avg_ticket_requester_id) AS max_avg_ticket_requester_id,
        SUM(CASE WHEN LOWER(agent_name) LIKE '%sa%' THEN 1 ELSE 0 END) AS n_lower_agent_name_like_sa
    FROM with_activity
    GROUP BY tier, TO_DATE(last_ticket_closed_at)
    HAVING MAX(hired_on_year) > 2025
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        tier,
        last_ticket_closed_at_day,
        COUNT(*) OVER (ORDER BY n_lower_agent_name_like_sa, tier, last_ticket_closed_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        n_lower_agent_name_like_sa - LAG(n_lower_agent_name_like_sa) OVER w_all_by_n_lower_agent_name_li_2 AS n_lower_agent_name_like_sa_change,
        SUM(max_avg_ticket_requester_id) OVER (PARTITION BY tier, MONTH(last_ticket_closed_at_day) ORDER BY max_avg_ticket_requester_id DESC, tier, last_ticket_closed_at_day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_avg_ticket_requester_id,
        LAG(last_ticket_closed_at_day, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', last_ticket_closed_at_day), n_lower_agent_name_like_sa ORDER BY n_lower_agent_name_like_sa, tier, last_ticket_closed_at_day) AS prev3_last_ticket_closed_at_day
    FROM totals
    WINDOW w_all_by_n_lower_agent_name_li_2 AS (ORDER BY n_lower_agent_name_like_sa, tier, last_ticket_closed_at_day)
)
-- Final projection
SELECT
    tier,
    last_ticket_closed_at_day,
    running_count,
    n_lower_agent_name_like_sa_change,
    running_max_avg_ticket_requester_id,
    prev3_last_ticket_closed_at_day,
    COALESCE(tier, 'UNKNOWN') AS tier_or_unknown,
    ABS(running_count - 10) AS running_count_distance
FROM with_trends
ORDER BY running_count_distance
