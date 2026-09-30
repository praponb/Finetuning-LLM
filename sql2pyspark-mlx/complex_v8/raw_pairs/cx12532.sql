WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type <> 'reopened'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_actor_by_event_at AS seq_no,
        RANK() OVER w_all_by_event_at_ties AS event_at_rank,
        NTILE(10) OVER (ORDER BY event_at ASC, id) AS event_at_decile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY actor ORDER BY event_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor ORDER BY event_at DESC NULLS FIRST, id), 3) AS event_at_percent_rank,
        DENSE_RANK() OVER w_actor_by_event_at AS event_at_dense_rank,
        LEAD(event_at, 3) OVER w_event_at_to_date_by_event_at AS next3_event_at,
        RANK() OVER w_event_at_month_by_event_at_t AS event_at_rank_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at ASC, id))) / 60, 1) AS minutes_since_prev_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_event_at_2,
        NTILE(10) OVER (PARTITION BY MONTH(event_at) ORDER BY event_at DESC, id) AS event_at_decile_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at_3
    FROM filtered
    WINDOW w_actor_by_event_at AS (PARTITION BY actor ORDER BY event_at DESC NULLS FIRST, id), w_all_by_event_at_ties AS (ORDER BY event_at ASC), w_event_at_to_date_by_event_at AS (PARTITION BY TO_DATE(event_at) ORDER BY event_at, id), w_event_at_month_by_event_at_t AS (PARTITION BY MONTH(event_at) ORDER BY event_at DESC)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        event_at_decile_2 + event_at_rank_2 AS event_at_decile_2_plus_event_at_rank_2,
        CASE WHEN event_at_decile_2 >= 7 THEN 'A' WHEN event_at_decile_2 >= 4 THEN 'B' ELSE 'C' END AS event_at_decile_2_tier
    FROM with_windows
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    seq_no,
    event_at_rank,
    event_at_decile,
    second_event_at,
    event_at_percent_rank,
    event_at_dense_rank,
    next3_event_at,
    event_at_rank_2,
    minutes_since_prev_event_at,
    event_at_decile_2,
    minutes_since_prev_event_at_3,
    event_at_decile_2_plus_event_at_rank_2,
    event_at_decile_2_tier,
    INITCAP(TRIM(event_type)) AS event_type_clean
FROM derived
WHERE event_at_rank_2 <= 6
