WITH filtered_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_label,
        ADD_MONTHS(hired_on, 12) AS hired_on_plus_12m
    FROM support_agents
    WHERE team IS NOT NULL
      AND agent_name IS NOT NULL
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT
        agent_id,
        MAX(requester_id) AS max_ticket_requester_id,
        COUNT(*) AS ticket_count,
        COUNT(*) AS ticket_count_2
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.max_ticket_requester_id,
        COALESCE(t.ticket_count, 0) AS ticket_count,
        COALESCE(t.ticket_count_2, 0) AS ticket_count_2
    FROM filtered_agents s
    LEFT JOIN ticket_stats t ON t.agent_id = s.id
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        LEAST(ticket_count_2, ticket_count) AS least_ticket_count_2_ticket_count,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM with_stats
    WHERE ticket_count >= 1
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        ABS(ticket_count_2 - 3) AS ticket_count_2_distance,
        DATE_FORMAT(hired_on_plus_12m, 'yyyy-MM') AS hired_on_plus_12m_label,
        GREATEST(ticket_count, ticket_count_2) AS greatest_ticket_count_ticket_count_2
    FROM labeled
)
SELECT
    ticket_count_2_distance,
    COALESCE(tier, 'ALL') AS tier,
    ticket_count,
    COUNT(*) AS n_rows,
    COUNT(DISTINCT tier) AS distinct_tiers,
    COUNT(DISTINCT id) AS distinct_ids
FROM annotated
GROUP BY CUBE(ticket_count_2_distance, tier, ticket_count)
ORDER BY ticket_count_2_distance NULLS LAST, tier NULLS LAST, ticket_count NULLS LAST
