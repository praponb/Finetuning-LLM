WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM support_agents
),
summarized_rows AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.id) AS ticket_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.id) AS last_ticket_closed_at,
        (SELECT MAX(t.opened_at) FROM tickets t WHERE t.agent_id = s.id) AS last_ticket_opened_at
    FROM valid s
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(last_ticket_opened_at, 'yyyy-MM') AS last_ticket_opened_at_ym,
        ticket_count,
        ROUND(AVG(ticket_count), 2) AS avg_ticket_count,
        MIN(last_ticket_closed_at) AS first_last_ticket_closed_at,
        COUNT(DISTINCT tier) AS distinct_tiers,
        COUNT(*) FILTER (WHERE LOWER(agent_name) LIKE '%ri%') AS n_lower_agent_name_like_ri
    FROM summarized_rows
    GROUP BY DATE_FORMAT(last_ticket_opened_at, 'yyyy-MM'), ticket_count
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE avg_ticket_count < (SELECT MAX(avg_ticket_count) FROM totals)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_activity
    WHERE ticket_count > (SELECT AVG(ticket_count) FROM with_activity)
)
SELECT
    ticket_count,
    distinct_tiers,
    n_lower_agent_name_like_ri,
    ROUND(STDDEV(ticket_count), 2) AS stddev_ticket_count,
    MIN(first_last_ticket_closed_at) AS first_first_last_ticket_closed_at
FROM without_matches
GROUP BY GROUPING SETS ((ticket_count, distinct_tiers, n_lower_agent_name_like_ri), (ticket_count), ())
ORDER BY ticket_count NULLS LAST, distinct_tiers NULLS LAST, n_lower_agent_name_like_ri NULLS LAST
