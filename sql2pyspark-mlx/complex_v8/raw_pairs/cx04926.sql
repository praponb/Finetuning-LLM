WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE ticket_id >= 3
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.csat AS ticket_csat
    FROM cleaned_events s
    JOIN tickets t ON t.id = s.ticket_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        event_type,
        ROW_NUMBER() OVER w_all_by_event_at AS event_at_seq,
        SUM(ticket_csat) OVER (PARTITION BY YEAR(event_at), ticket_id) AS event_at_year_ticket_id_sum_ticket_csat,
        ROUND(ticket_csat - AVG(ticket_csat) OVER w_event_at_year_ticket_id, 2) AS ticket_csat_vs_event_at_year_ticket_id_avg,
        RANK() OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY ticket_csat) AS ticket_csat_rank,
        ROUND(ticket_csat / NULLIF(SUM(ticket_csat) OVER (PARTITION BY ticket_csat), 0), 4) AS ticket_csat_share_of_ticket_csat,
        MAX(ticket_csat) OVER (PARTITION BY YEAR(event_at), ticket_id) AS event_at_year_ticket_id_max_ticket_csat,
        SUM(ticket_csat) OVER (PARTITION BY actor, TO_DATE(event_at) ORDER BY ticket_csat, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS ticket_csat_sum_1_prior,
        MIN(ticket_csat) OVER (PARTITION BY actor) AS actor_min_ticket_csat,
        LEAD(event_at, 2) OVER (ORDER BY event_at DESC, id) AS next2_event_at
    FROM with_details
    WINDOW w_all_by_event_at AS (ORDER BY event_at ASC, id), w_event_at_year_ticket_id AS (PARTITION BY YEAR(event_at), ticket_id)
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        event_at_year_ticket_id_sum_ticket_csat,
        ticket_csat_vs_event_at_year_ticket_id_avg,
        ticket_csat_rank,
        event_at_year_ticket_id_max_ticket_csat,
        actor_min_ticket_csat,
        next2_event_at,
        MAX(event_at_seq) OVER (ORDER BY event_at_seq ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_event_at_seq_so_far,
        DENSE_RANK() OVER (PARTITION BY event_at_seq ORDER BY event_at_seq DESC, id) AS event_at_seq_dense_rank,
        ROUND(AVG(ticket_csat_rank) OVER w_event_type_ticket_csat_rank, 2) AS event_type_ticket_csat_rank_avg_ticket_csat_rank,
        DENSE_RANK() OVER w_event_at_seq_by_event_at_seq_2 AS event_at_seq_dense_rank_2,
        SUM(actor_min_ticket_csat) OVER (PARTITION BY event_type, event_at_seq ORDER BY ticket_csat_sum_1_prior, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS actor_min_ticket_csat_sum_2_prior
    FROM sequenced
    WINDOW w_event_type_ticket_csat_rank AS (PARTITION BY event_type, ticket_csat_rank), w_event_at_seq_by_event_at_seq_2 AS (PARTITION BY event_at_seq ORDER BY event_at_seq DESC)
)
-- Final projection
SELECT
    id,
    event_at_year_ticket_id_sum_ticket_csat,
    ticket_csat_vs_event_at_year_ticket_id_avg,
    event_at_year_ticket_id_max_ticket_csat,
    actor_min_ticket_csat,
    next2_event_at,
    event_at_seq_dense_rank,
    event_type_ticket_csat_rank_avg_ticket_csat_rank,
    ROUND(max_event_at_seq_so_far / NULLIF(actor_min_ticket_csat, 0), 3) AS max_event_at_seq_so_far_per_actor_min_ticket_csa,
    MONTH(next2_event_at) AS next2_event_at_month
FROM sequenced_2
ORDER BY next2_event_at
