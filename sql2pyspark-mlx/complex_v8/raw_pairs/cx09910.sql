WITH valid_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.channel AS ticket_channel,
        t.csat AS ticket_csat
    FROM valid_events s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel NOT IN ('phone', 'email')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.last_event_at_2
    FROM enriched_ticket s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(ticket_channel), ' ', '_')) AS ticket_channel_slug,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        last_event_at + INTERVAL 7 DAYS AS last_event_at_plus_7d
    FROM profiled
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        ticket_channel,
        last_event_at,
        last_event_at_2,
        ticket_channel_slug,
        event_type_slug,
        last_event_at_plus_7d,
        LENGTH(TRIM(ticket_channel)) AS ticket_channel_len,
        CONCAT(actor, '#', CAST(ticket_csat AS STRING)) AS actor_ref,
        CASE WHEN ticket_csat >= 4 THEN 'HIGH' WHEN ticket_csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS ticket_csat_tier
    FROM labeled
    WHERE last_event_at_plus_7d >= TIMESTAMP '2025-01-21 09:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ticket_channel_len / NULLIF(SUM(ticket_channel_len) OVER (PARTITION BY actor, ticket_channel), 0), 4) AS ticket_channel_len_share_of_actor_ticket_channel,
        MAX(ticket_channel_len) OVER (ORDER BY last_event_at_plus_7d DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ticket_channel_len_max_3_prior,
        SUM(ticket_channel_len) OVER (ORDER BY last_event_at_plus_7d DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS ticket_channel_len_sum_3_prior
    FROM scored
),
with_parent AS (
    -- Join tickets
    SELECT
        s.*,
        t.priority AS ticket_priority,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at
    FROM windowed s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT id, field, value
FROM (
    SELECT id, ticket_requester_id, ticket_channel_len_max_3_prior, ticket_channel_len_sum_3_prior
    FROM with_parent
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (ticket_requester_id, ticket_channel_len_max_3_prior, ticket_channel_len_sum_3_prior)
) unp
