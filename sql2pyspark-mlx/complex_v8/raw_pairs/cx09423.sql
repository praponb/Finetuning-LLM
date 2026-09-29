WITH recent_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        LENGTH(TRIM(actor)) AS actor_len,
        YEAR(event_at) AS event_at_year
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 10:00:00'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor_len,
        s.event_at_year,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel,
        t.requester_id AS ticket_requester_id,
        su.tier AS agent_tier
    FROM recent_events s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
),
per_group AS (
    -- Summarise per group
    SELECT
        ticket_id,
        agent_tier,
        MIN(actor_len) AS min_actor_len,
        COUNT(*) AS cnt
    FROM enriched
    WHERE ticket_opened_at >= TIMESTAMP '2025-02-06 06:00:00'
    GROUP BY ticket_id, agent_tier
),
derived AS (
    -- Derive helper columns
    SELECT *, ROUND(min_actor_len / NULLIF(cnt, 0), 3) AS min_actor_len_per_cnt
    FROM per_group
),
combined_details_ticket AS (
    -- Join tickets
    SELECT
        s.ticket_id,
        s.agent_tier,
        s.min_actor_len,
        t.closed_at AS ticket_closed_at,
        t.opened_at AS ticket_opened_at,
        t.csat AS ticket_csat,
        su.agent_name
    FROM derived s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE HOUR(t.opened_at) BETWEEN 7 AND 9
),
with_parent AS (
    -- Join tickets
    SELECT
        s.ticket_id,
        s.agent_tier,
        s.min_actor_len,
        s.ticket_closed_at,
        t.csat AS ticket_csat_2,
        t.channel AS ticket_channel
    FROM combined_details_ticket s
    JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    ticket_id,
    agent_tier,
    min_actor_len,
    ticket_closed_at,
    ticket_csat_2,
    SUBSTRING(TRIM(ticket_channel), 1, 4) AS ticket_channel_prefix,
    DATE_TRUNC('MONTH', ticket_closed_at) AS ticket_closed_at_month
FROM with_parent
