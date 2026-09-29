WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE actor = 'system'
      AND event_at >= TIMESTAMP '2025-01-31 09:00:00'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id) AS next_event_at,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS event_at_seq,
        LAG(event_at) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id) AS prev_event_at,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        DENSE_RANK() OVER w_event_type_ticket_id_by_even AS event_at_dense_rank,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAG(event_at, 3) OVER (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id) AS prev3_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id), 3) AS event_at_percent_rank_2
    FROM base
    WINDOW w_event_type_ticket_id_by_even AS (PARTITION BY event_type, ticket_id ORDER BY event_at ASC, id)
),
leaders AS (
    -- Top 5 per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        next_event_at,
        event_at_percent_rank,
        first_event_at,
        event_at_seq,
        prev_event_at,
        last_event_at,
        last_known_event_at,
        first_known_event_at,
        second_event_at,
        prev3_event_at,
        event_at_percent_rank_2
    FROM windowed
    WHERE event_at_dense_rank <= 5
      AND prev3_event_at >= TIMESTAMP '2025-01-25 10:00:00'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_event_at_seq_ties, 3) AS event_at_seq_cume_dist,
        ROUND(AVG(event_at_percent_rank) OVER (ORDER BY event_at_seq DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS event_at_percent_rank_avg_3_prior,
        RANK() OVER (ORDER BY event_at_seq DESC, id) AS event_at_seq_rank,
        ROUND(PERCENT_RANK() OVER w_all_by_event_at_seq_ties, 3) AS event_at_seq_percent_rank,
        MIN(event_at_percent_rank_2) OVER (PARTITION BY TO_DATE(first_event_at), event_at_seq) AS first_event_at_to_date_event_at_seq_min_event_at,
        SIZE(COLLECT_SET(event_type) OVER (PARTITION BY TO_DATE(first_event_at), event_at_seq)) AS distinct_event_type_in_first_event_at_to_date_ev
    FROM leaders
    WINDOW w_all_by_event_at_seq_ties AS (ORDER BY event_at_seq DESC)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    next_event_at,
    event_at_percent_rank,
    first_event_at,
    event_at_seq,
    last_known_event_at,
    second_event_at,
    event_at_percent_rank_2,
    event_at_seq_cume_dist,
    event_at_percent_rank_avg_3_prior,
    event_at_seq_percent_rank,
    first_event_at_to_date_event_at_seq_min_event_at,
    distinct_event_type_in_first_event_at_to_date_ev,
    event_at_seq_cume_dist - event_at_percent_rank AS event_at_seq_cume_dist_minus_event_at_percent_ra,
    LENGTH(TRIM(actor)) AS actor_len
FROM metrics
ORDER BY event_at_seq_percent_rank DESC NULLS FIRST, first_event_at DESC NULLS FIRST, id
LIMIT 20
