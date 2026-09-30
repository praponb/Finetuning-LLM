WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 2) AS actor_prefix,
        YEAR(event_at) AS event_at_year,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE (event_at >= TIMESTAMP '2025-01-31 13:00:00' OR actor NOT IN ('customer', 'agent'))
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor_prefix,
        actor_event_type_label,
        LAST_VALUE(event_at_year) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_year,
        LAST_VALUE(event_at) OVER (PARTITION BY event_type ORDER BY event_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        ROUND(event_at_year - AVG(event_at_year) OVER (PARTITION BY ticket_id), 2) AS event_at_year_vs_ticket_id_avg,
        LAG(event_at_year, 2, 0) OVER (PARTITION BY event_at_year ORDER BY event_at_year DESC NULLS FIRST, id) AS prev2_event_at_year,
        MIN(event_at_year) OVER (PARTITION BY ticket_id) AS ticket_id_min_event_at_year
    FROM scoped
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(actor_prefix, '#', CAST(last_known_event_at_year AS STRING)) AS actor_prefix_ref,
        DATE_TRUNC('YEAR', last_event_at) AS last_event_at_year
    FROM ranked
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM derived s
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.actor_prefix,
        s.actor_event_type_label,
        s.last_known_event_at_year,
        s.prev2_event_at_year,
        s.ticket_id_min_event_at_year,
        s.actor_prefix_ref,
        s.event_count,
        t.agent_id AS ticket_agent_id,
        t.closed_at AS ticket_closed_at,
        t.channel AS ticket_channel
    FROM summarized_rows s
    JOIN tickets t ON t.id = s.ticket_id
),
latest AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        actor_prefix,
        actor_event_type_label,
        last_known_event_at_year,
        prev2_event_at_year,
        ticket_id_min_event_at_year,
        actor_prefix_ref,
        event_count,
        ticket_agent_id,
        ticket_closed_at,
        ticket_channel
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY ticket_closed_at DESC, id DESC) AS row_num
        FROM with_details
    ) ranked
    WHERE row_num = 1
),
latest_2 AS (
    -- Keep the latest row per ticket_channel, ticket_agent_id
    SELECT
        id,
        ticket_id,
        actor_prefix,
        actor_event_type_label,
        last_known_event_at_year,
        prev2_event_at_year,
        ticket_id_min_event_at_year,
        actor_prefix_ref,
        event_count,
        ticket_agent_id,
        ticket_closed_at,
        ticket_channel
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_channel, ticket_agent_id ORDER BY last_known_event_at_year DESC, id DESC) AS recency_rank
        FROM latest
    ) ranked
    WHERE recency_rank = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor_prefix,
    prev2_event_at_year,
    ticket_id_min_event_at_year,
    actor_prefix_ref,
    event_count,
    ticket_agent_id,
    ticket_closed_at,
    CONCAT(actor_event_type_label, '#', CAST(ticket_id AS STRING)) AS actor_event_type_label_ref
FROM latest_2
WHERE actor_prefix IS NOT NULL
