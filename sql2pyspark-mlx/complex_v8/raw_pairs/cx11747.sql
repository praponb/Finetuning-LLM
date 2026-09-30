WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d,
        LENGTH(TRIM(event_type)) AS event_type_len
    FROM ticket_events
    WHERE event_type NOT IN ('created', 'escalated')
      AND ticket_id IS NOT NULL
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS event_at_seq,
        COUNT(*) OVER (PARTITION BY ticket_id) AS rows_in_ticket_id,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY event_type_len, ticket_id)) AS distinct_event_type_in_event_type_len_ticket_id,
        LEAD(event_type_len) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_plus_7d DESC, id) AS next_event_type_len,
        DENSE_RANK() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC) AS event_at_dense_rank,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank_2,
        SUM(event_type_len) OVER (PARTITION BY actor ORDER BY event_at_plus_7d DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_type_len,
        SUM(event_type_len) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_plus_7d DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_type_len_sum_4,
        MAX(event_type_len) OVER (PARTITION BY ticket_id) AS ticket_id_max_event_type_len,
        LAG(event_type_len) OVER (PARTITION BY actor ORDER BY event_at_plus_7d DESC NULLS FIRST, id) AS prev_event_type_len,
        ROUND(event_type_len / NULLIF(SUM(event_type_len) OVER (PARTITION BY ticket_id), 0), 4) AS event_type_len_share_of_ticket_id,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY event_type_len, ticket_id)) AS distinct_actor_in_event_type_len_ticket_id
    FROM cleaned_events
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(event_at_dense_rank) OVER (PARTITION BY event_at_dense_rank ORDER BY event_type_len_sum_4, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_at_dense_rank_sum_6_prior,
        ROUND(running_event_type_len - AVG(running_event_type_len) OVER w_event_at_seq, 2) AS running_event_type_len_vs_event_at_seq_avg,
        ROUND(event_at_dense_rank / NULLIF(SUM(event_at_dense_rank) OVER (PARTITION BY event_at_seq), 0), 4) AS event_at_dense_rank_share_of_event_at_seq
    FROM windowed
    WINDOW w_event_at_seq AS (PARTITION BY event_at_seq)
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_at_plus_7d,
    distinct_event_type_in_event_type_len_ticket_id,
    next_event_type_len,
    event_at_dense_rank,
    event_type_len_sum_4,
    prev_event_type_len,
    event_at_dense_rank_sum_6_prior,
    running_event_type_len_vs_event_at_seq_avg,
    event_at_dense_rank_share_of_event_at_seq,
    TO_DATE(event_at_plus_7d) AS event_at_plus_7d_date
FROM ranked
ORDER BY event_at_dense_rank_share_of_event_at_seq DESC, id
LIMIT 20
