WITH prepared_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        DATE_TRUNC('QUARTER', opened_at) AS opened_at_quarter,
        COALESCE(csat, 0) AS csat_filled
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
      AND requester_id BETWEEN 2 AND 21
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM prepared_tickets s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
        )
),
with_parent_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.opened_at_quarter,
        s.csat_filled,
        su.tier AS agent_tier
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.tier NOT IN ('T2', 'T3')
),
agent_tier_stats AS (
    -- Max csat_filled per agent_tier
    SELECT agent_tier, MAX(csat_filled) AS max_csat_filled
    FROM with_parent_agent
    GROUP BY agent_tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_csat_filled,
        ROUND(s.csat_filled - g.max_csat_filled, 2) AS csat_filled_minus_max
    FROM with_parent_agent s
    JOIN agent_tier_stats g ON g.agent_tier = s.agent_tier
    WHERE s.csat_filled = g.max_csat_filled
),
combined_details_agent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team
    FROM above_average s
    JOIN support_agents su ON su.id = s.agent_id
),
totals AS (
    -- Group-level totals
    SELECT
        csat_filled,
        priority,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
        ROUND(SUM(csat_filled) / NULLIF(SUM(requester_id), 0), 3) AS csat_filled_to_requester_id_ratio,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers
    FROM combined_details_agent
    GROUP BY 1, 2
)
SELECT
    csat_filled,
    priority,
    row_count,
    distinct_agent_tiers,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
    ARRAY_JOIN(ARRAY_SORT(agent_team_list), ',') AS agent_team_list_text
FROM totals
ORDER BY csat_filled NULLS LAST, priority
LIMIT 5
