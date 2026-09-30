WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        MONTH(event_at) AS event_at_month_2
    FROM ticket_events
    WHERE event_type IN ('reopened', 'resolved')
      AND actor IN ('agent', 'system')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at
    FROM scoped_events s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        actor,
        event_at_month,
        last_event_at,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at, 3) AS event_at_percent_rank,
        NTILE(3) OVER (ORDER BY event_at, id) AS event_at_tercile
    FROM with_stats
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id)
),
joined_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags,
        t.closed_at AS ticket_closed_at
    FROM windowed s
    JOIN tickets t ON t.id = s.ticket_id
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        DATE_TRUNC('MONTH', ticket_closed_at) AS ticket_closed_at_month,
        COUNT(*) AS cnt,
        MIN(event_at_percent_rank) AS min_event_at_percent_rank,
        ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
        MIN(last_event_at) AS first_last_event_at
    FROM joined_ticket
    GROUP BY ticket_id, DATE_TRUNC('MONTH', ticket_closed_at)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(first_last_event_at) AS first_last_event_at_date,
        DATE_TRUNC('QUARTER', first_last_event_at) AS first_last_event_at_quarter
    FROM stats
    WHERE cnt < 8
)
SELECT
    ticket_id,
    ticket_closed_at_month,
    min_event_at_percent_rank,
    actor_list,
    first_last_event_at,
    first_last_event_at_quarter,
    COALESCE(cnt, 0) AS cnt_filled,
    ARRAY_JOIN(ARRAY_SORT(actor_list), ',') AS actor_list_text
FROM tagged
ORDER BY cnt_filled NULLS LAST, ticket_id, ticket_closed_at_month
LIMIT 20
