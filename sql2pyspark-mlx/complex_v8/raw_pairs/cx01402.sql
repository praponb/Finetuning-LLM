WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor IS NOT NULL
),
enriched AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_clean,
        s.actor_len,
        t.channel AS ticket_channel,
        su.agent_name
    FROM filtered_events s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.channel = 'email'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY ticket_channel, MONTH(event_at))) AS distinct_event_type_in_ticket_channel_event_at_m,
        NTH_VALUE(actor_len, 2) OVER (PARTITION BY TO_DATE(event_at), actor_len ORDER BY event_at ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_actor_len,
        RANK() OVER (PARTITION BY TO_DATE(event_at), actor_len ORDER BY event_at ASC NULLS LAST, id) AS event_at_rank
    FROM enriched
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM with_windows s
),
enriched_2 AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(ticket_channel, '#', CAST(id AS STRING)) AS ticket_channel_ref,
        actor_len - event_at_rank AS actor_len_minus_event_at_rank
    FROM with_counts
),
event_stats_2 AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at_2,
        MAX(event_at) AS last_event_at_3,
        COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at_2,
        t.last_event_at_3,
        COALESCE(t.event_count, 0) AS event_count
    FROM enriched_2 s
    LEFT JOIN event_stats_2 t ON t.ticket_id = s.ticket_id
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_clean,
        s.actor_len,
        s.ticket_channel,
        s.distinct_event_type_in_ticket_channel_event_at_m,
        s.second_actor_len,
        s.ticket_channel_ref,
        s.actor_len_minus_event_at_rank,
        s.last_event_at_2,
        s.event_count,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        su.team AS agent_team
    FROM summarized_rows s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, second_actor_len, distinct_event_type_in_ticket_channel_event_at_m, event_count, actor_len_minus_event_at_rank
    FROM combined_details
) src
UNPIVOT (
    amount FOR field IN (second_actor_len AS m_second_actor_len, distinct_event_type_in_ticket_channel_event_at_m AS distinct_event_type_in_ticket_channel_event_at_m_2, event_count AS m_event_count, actor_len_minus_event_at_rank AS actor_len_minus_event_at_rank_v)
) unp
