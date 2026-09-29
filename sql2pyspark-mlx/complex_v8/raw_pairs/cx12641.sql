WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'customer'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.opened_at AS ticket_opened_at
    FROM valid s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at,
        event_type,
        actor,
        ticket_opened_at,
        NTILE(10) OVER (PARTITION BY ticket_id, actor ORDER BY ticket_opened_at ASC, id) AS ticket_opened_at_decile,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(ticket_opened_at) ORDER BY ticket_opened_at, id) AS ticket_opened_at_seq,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, actor ORDER BY ticket_opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_opened_at DESC), 3) AS ticket_opened_at_percent_rank,
        LAG(ticket_opened_at, 3) OVER (PARTITION BY ticket_id, actor ORDER BY ticket_opened_at ASC, id) AS prev3_ticket_opened_at
    FROM with_details
),
event_type_stats AS (
    -- Avg ticket_opened_at_seq per event_type
    SELECT event_type, AVG(ticket_opened_at_seq) AS avg_ticket_opened_at_seq
    FROM with_trends
    GROUP BY event_type
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ticket_opened_at_seq,
        ROUND(s.ticket_opened_at_seq - g.avg_ticket_opened_at_seq, 2) AS ticket_opened_at_seq_minus_avg
    FROM with_trends s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.ticket_opened_at_seq > g.avg_ticket_opened_at_seq
),
deduped AS (
    -- Keep the earliest row per event_type, actor
    SELECT
        id,
        event_at,
        event_type,
        actor,
        ticket_opened_at,
        ticket_opened_at_decile,
        ticket_opened_at_seq,
        last_known_event_at,
        ticket_opened_at_percent_rank,
        prev3_ticket_opened_at,
        avg_ticket_opened_at_seq,
        ticket_opened_at_seq_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY ticket_opened_at_decile, id) AS row_num
        FROM vs_group
    ) ranked
    WHERE row_num = 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        actor,
        ticket_opened_at_decile,
        ticket_opened_at_seq,
        last_known_event_at,
        prev3_ticket_opened_at,
        avg_ticket_opened_at_seq,
        ticket_opened_at_seq_minus_avg,
        LENGTH(TRIM(event_type)) AS event_type_len,
        DATEDIFF(DATE '2025-06-30', prev3_ticket_opened_at) AS days_since_prev3_ticket_opened_at
    FROM deduped
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    ticket_opened_at_decile,
    ticket_opened_at_seq,
    prev3_ticket_opened_at,
    avg_ticket_opened_at_seq,
    ticket_opened_at_seq_minus_avg,
    days_since_prev3_ticket_opened_at,
    ROUND(event_type_len / NULLIF(avg_ticket_opened_at_seq, 0), 3) AS event_type_len_per_avg_ticket_opened_at_seq
FROM annotated
ORDER BY event_type_len_per_avg_ticket_opened_at_seq, ticket_opened_at_seq_minus_avg DESC NULLS FIRST, id
LIMIT 20
