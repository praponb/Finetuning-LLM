WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_type = 'replied'
      AND HOUR(event_at) BETWEEN 8 AND 13
),
with_details_ticket AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.event_at_date,
        s.actor_ref,
        t.opened_at AS ticket_opened_at,
        t.channel AS ticket_channel
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.channel <> 'email'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_type,
        actor,
        actor_ref,
        ticket_opened_at,
        ticket_channel,
        ROUND(CUME_DIST() OVER (ORDER BY ticket_opened_at DESC, id), 3) AS ticket_opened_at_cume_dist,
        ROW_NUMBER() OVER w_ticket_channel_by_event_at_d AS seq_no,
        NTILE(5) OVER (ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_quintile,
        DENSE_RANK() OVER w_all_by_ticket_opened_at AS ticket_opened_at_dense_rank,
        LEAD(event_at_date) OVER (PARTITION BY event_type ORDER BY ticket_opened_at, id) AS next_event_at_date,
        LAG(ticket_opened_at, 2) OVER (ORDER BY ticket_opened_at DESC, id) AS prev2_ticket_opened_at,
        ROW_NUMBER() OVER (ORDER BY ticket_opened_at DESC, id) AS seq_no_2,
        DENSE_RANK() OVER w_event_type_by_ticket_opened AS ticket_opened_at_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (ORDER BY ticket_opened_at DESC), 3) AS ticket_opened_at_percent_rank,
        NTILE(4) OVER (ORDER BY ticket_opened_at DESC, id) AS ticket_opened_at_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_channel ORDER BY event_at_date, id), 3) AS event_at_date_percent_rank,
        RANK() OVER (PARTITION BY event_type ORDER BY ticket_opened_at) AS ticket_opened_at_rank
    FROM with_details_ticket
    WINDOW w_ticket_channel_by_event_at_d AS (PARTITION BY ticket_channel ORDER BY event_at_date, id), w_all_by_ticket_opened_at AS (ORDER BY ticket_opened_at DESC, id), w_event_type_by_ticket_opened AS (PARTITION BY event_type ORDER BY ticket_opened_at, id)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(ticket_opened_at_dense_rank) OVER (PARTITION BY MONTH(prev2_ticket_opened_at), ticket_opened_at_quartile) AS prev2_ticket_opened_at_month_ticket_opened_at_qu,
        RANK() OVER (PARTITION BY seq_no ORDER BY ticket_opened_at_rank) AS ticket_opened_at_rank_rank,
        DENSE_RANK() OVER (PARTITION BY seq_no ORDER BY ticket_opened_at_rank) AS ticket_opened_at_rank_dense_rank
    FROM with_windows
)
-- Final projection
SELECT
    id,
    event_type,
    actor,
    actor_ref,
    ticket_opened_at,
    ticket_channel,
    ticket_opened_at_cume_dist,
    ticket_opened_at_dense_rank,
    next_event_at_date,
    prev2_ticket_opened_at,
    seq_no_2,
    ticket_opened_at_dense_rank_2,
    ticket_opened_at_quartile,
    event_at_date_percent_rank,
    ticket_opened_at_rank,
    ticket_opened_at_rank_rank,
    ticket_opened_at_rank_dense_rank,
    CONCAT(event_type, '#', CAST(id AS STRING)) AS event_type_ref,
    LEAST(ticket_opened_at_rank, seq_no_2) AS least_ticket_opened_at_rank_seq_no_2
FROM with_windows_2
ORDER BY ticket_opened_at_dense_rank_2
