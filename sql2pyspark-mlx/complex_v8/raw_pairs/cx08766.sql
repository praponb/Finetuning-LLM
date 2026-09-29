WITH cleaned AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE event_type = 'created'
      AND ticket_id <= 54
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_len,
        s.actor_slug,
        t.agent_id AS ticket_agent_id,
        t.requester_id AS ticket_requester_id,
        t.tags AS ticket_tags
    FROM cleaned s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE ARRAY_CONTAINS(t.tags, 'outage')
),
combined_details_ticket_2 AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_slug,
        s.ticket_agent_id,
        s.ticket_requester_id,
        s.ticket_tags,
        t.opened_at AS ticket_opened_at,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM combined_details_ticket s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = s.ticket_agent_id
),
per_group AS (
    -- Group-level totals
    SELECT
        event_type,
        DATE_FORMAT(agent_hired_on, 'yyyy-MM') AS agent_hired_on_ym,
        PERCENTILE_APPROX(ticket_requester_id, 0.5) AS median_ticket_requester_id,
        COUNT(*) AS cnt,
        COUNT(DISTINCT ticket_id) AS distinct_ticket_ids,
        MIN(ticket_requester_id) AS min_ticket_requester_id
    FROM combined_details_ticket_2
    GROUP BY event_type, DATE_FORMAT(agent_hired_on, 'yyyy-MM')
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN event_type IN ('replied', 'resolved') THEN 'group_a' WHEN event_type = 'reopened' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM per_group
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(event_type_group, '#', CAST(cnt AS STRING)) AS event_type_group_ref,
        LENGTH(TRIM(event_type_group)) AS event_type_group_len,
        ROUND(cnt / NULLIF(min_ticket_requester_id, 0), 3) AS cnt_per_min_ticket_requester_id
    FROM labeled
)
-- Final projection
SELECT
    event_type,
    agent_hired_on_ym,
    median_ticket_requester_id,
    cnt,
    distinct_ticket_ids,
    min_ticket_requester_id,
    event_type_group,
    event_type_group_ref,
    cnt_per_min_ticket_requester_id,
    cnt + distinct_ticket_ids AS cnt_plus_distinct_ticket_ids
FROM annotated
WHERE event_type IS NOT NULL
ORDER BY event_type DESC, event_type_group DESC
