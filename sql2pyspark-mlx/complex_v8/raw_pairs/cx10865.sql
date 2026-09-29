WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_at >= TIMESTAMP '2025-01-31 13:00:00'
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM prepared_events s
),
unique_rows AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        last_event_at,
        last_event_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY last_event_at_2, id) AS row_num
        FROM with_stats
    ) ranked
    WHERE row_num = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_ticket_id_by_last_event_at_t, 3) AS last_event_at_percent_rank,
        RANK() OVER w_last_event_at_year_event_typ AS event_at_rank
    FROM unique_rows
    WINDOW w_ticket_id_by_last_event_at_t AS (PARTITION BY ticket_id ORDER BY last_event_at ASC), w_last_event_at_year_event_typ AS (PARTITION BY YEAR(last_event_at), event_type ORDER BY event_at ASC, id)
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.opened_at AS ticket_opened_at, su.team AS agent_team
    FROM with_trends s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE TO_DATE(t.opened_at) < DATE '2025-02-18'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(last_event_at_2) - UNIX_TIMESTAMP(last_event_at)) / 60, 1) AS minutes_last_event_at_to_last_event_at_2,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM combined_details_ticket
    WHERE TO_DATE(last_event_at_2) < DATE '2025-02-17'
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT actor_or_unknown, event_type, last_event_at_percent_rank
    FROM scored
) src
PIVOT (
    MIN(last_event_at_percent_rank) AS min_v, SUM(last_event_at_percent_rank) AS sum_v
    FOR event_type IN ('assigned', 'resolved', 'reopened')
)
ORDER BY actor_or_unknown
