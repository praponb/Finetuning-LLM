WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        LOWER(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.actor IN ('system', 'customer')) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count
    FROM recent s
),
unique_rows AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_or_unknown,
        event_type_clean,
        event_actor_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC, id DESC) AS row_num
        FROM with_counts
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type_or_unknown ORDER BY event_at DESC, id) AS event_at_seq,
        MAX(event_actor_count) OVER (PARTITION BY event_actor_count) AS event_actor_count_max_event_actor_count,
        NTILE(3) OVER w_ticket_id_event_type_or_unkn AS event_at_tercile,
        ROUND(AVG(event_actor_count) OVER (PARTITION BY event_actor_count), 2) AS event_actor_count_avg_event_actor_count,
        MIN(event_actor_count) OVER (PARTITION BY event_actor_count) AS event_actor_count_min_event_actor_count
    FROM unique_rows
    WINDOW w_ticket_id_event_type_or_unkn AS (PARTITION BY ticket_id, event_type_or_unknown ORDER BY event_at DESC, id)
),
with_parent AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_type_clean,
        s.event_actor_count_max_event_actor_count,
        s.event_actor_count_min_event_actor_count,
        t.requester_id AS ticket_requester_id,
        t.csat AS ticket_csat,
        su.tier AS agent_tier
    FROM with_trends s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
joined AS (
    -- Attach ticket attributes
    SELECT s.*, t.csat AS ticket_csat_2
    FROM with_parent s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM joined s
)
SELECT
    id,
    ticket_id,
    event_type,
    event_type_clean,
    ticket_csat,
    agent_tier,
    ticket_csat_2,
    first_event_at,
    last_event_at_2,
    COALESCE(ticket_csat_2, 0) AS ticket_csat_2_filled
FROM summarized_rows
