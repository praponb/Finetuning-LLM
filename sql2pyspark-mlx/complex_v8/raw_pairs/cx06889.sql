WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE actor IN ('system', 'agent')
),
with_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor_prefix,
        s.actor_slug,
        t.channel AS ticket_channel,
        t.agent_id AS ticket_agent_id
    FROM prepared s
    JOIN tickets t ON t.id = s.ticket_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        ticket_channel,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        ARRAY_SORT(COLLECT_SET(ticket_channel)) AS ticket_channel_list,
        COUNT(*) FILTER (WHERE event_type IS NOT NULL) AS n_event_type_is_not_null
    FROM with_details_ticket
    WHERE ticket_id <= 23
    GROUP BY ticket_channel
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(n_event_type_is_not_null) OVER (ORDER BY n_event_type_is_not_null, ticket_channel ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS n_event_type_is_not_null_max_2_prior,
        NTH_VALUE(n_event_type_is_not_null, 2) OVER (ORDER BY n_event_type_is_not_null, ticket_channel ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_n_event_type_is_not_null,
        NTILE(5) OVER w_all_by_n_event_type_is_not_n_3 AS n_event_type_is_not_null_quintile
    FROM rollup_base
    WINDOW w_all_by_n_event_type_is_not_n_3 AS (ORDER BY n_event_type_is_not_null, ticket_channel)
)
SELECT
    ticket_channel,
    event_type_list,
    ticket_channel_list,
    n_event_type_is_not_null,
    n_event_type_is_not_null_max_2_prior,
    second_n_event_type_is_not_null,
    n_event_type_is_not_null_quintile,
    COALESCE(n_event_type_is_not_null, 0) AS n_event_type_is_not_null_filled
FROM with_trends
WHERE ticket_channel = 'chat'
ORDER BY n_event_type_is_not_null, n_event_type_is_not_null_max_2_prior DESC
