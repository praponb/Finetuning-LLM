WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(TRIM(event_type)) AS event_type_clean,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        t.closed_at AS ticket_closed_at,
        su.tier AS agent_tier,
        su.hired_on AS agent_hired_on
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
actor_group_stats AS (
    -- Avg ticket_csat per actor_group
    SELECT actor_group, AVG(ticket_csat) AS avg_ticket_csat
    FROM combined_details_ticket
    GROUP BY actor_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ticket_csat,
        ROUND(s.ticket_csat - g.avg_ticket_csat, 2) AS ticket_csat_minus_avg
    FROM combined_details_ticket s
    JOIN actor_group_stats g ON g.actor_group = s.actor_group
    WHERE s.ticket_csat > g.avg_ticket_csat
),
latest_per_key AS (
    -- Keep the earliest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        actor_group,
        ticket_requester_id,
        ticket_csat,
        ticket_closed_at,
        agent_tier,
        agent_hired_on,
        avg_ticket_csat,
        ticket_csat_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY ticket_csat_minus_avg, id) AS rn
        FROM vs_group
    ) ranked
    WHERE rn = 1
),
latest AS (
    -- Keep the latest row per agent_tier, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_clean,
        actor_group,
        ticket_requester_id,
        ticket_csat,
        ticket_closed_at,
        agent_tier,
        agent_hired_on,
        avg_ticket_csat,
        ticket_csat_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_tier, actor ORDER BY agent_hired_on DESC, id DESC) AS recency_rank
        FROM latest_per_key
    ) ranked
    WHERE recency_rank = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_group,
        ticket_requester_id,
        ticket_csat,
        ticket_csat_minus_avg,
        LOWER(REGEXP_REPLACE(TRIM(agent_tier), ' ', '_')) AS agent_tier_slug,
        FLOOR(ticket_csat_minus_avg / 2) * 2 AS ticket_csat_minus_avg_bucket
    FROM latest
    WHERE event_type_clean IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_group,
    ticket_requester_id,
    ticket_csat,
    ticket_csat_minus_avg,
    agent_tier_slug,
    ticket_csat_minus_avg_bucket,
    CASE WHEN actor IN ('system', 'customer') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group_2
FROM enriched
ORDER BY agent_tier_slug NULLS LAST
