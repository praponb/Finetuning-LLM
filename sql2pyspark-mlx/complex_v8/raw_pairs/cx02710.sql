WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean,
        LOWER(TRIM(actor)) AS actor_clean_2
    FROM ticket_events
    WHERE ticket_id >= 23
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_clean,
        actor_clean_2,
        RANK() OVER w_ticket_id_event_at_month_by AS event_at_rank,
        LAG(event_at, 2) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev2_event_at,
        ROUND(CUME_DIST() OVER w_all_by_event_at_ties, 3) AS event_at_cume_dist,
        FIRST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY ticket_id ORDER BY event_at, id), 3) AS event_at_cume_dist_2,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at ASC, id), 3) AS event_at_cume_dist_3,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_2,
        LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at ASC NULLS LAST, id) AS prev_event_at,
        LAG(event_at) OVER w_ticket_id_by_event_at AS prev_event_at_2,
        RANK() OVER w_event_type_by_event_at_ties AS event_at_rank_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at ASC NULLS LAST, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM prepared
    WINDOW w_ticket_id_event_at_month_by AS (PARTITION BY ticket_id, DATE_TRUNC('MONTH', event_at) ORDER BY event_at NULLS LAST), w_all_by_event_at_ties AS (ORDER BY event_at DESC), w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at, id), w_event_type_by_event_at_ties AS (PARTITION BY event_type ORDER BY event_at ASC)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_clean,
        actor_clean_2,
        event_at_rank,
        prev2_event_at,
        event_at_cume_dist,
        first_event_at,
        event_at_cume_dist_2,
        event_at_cume_dist_3,
        second_event_at,
        last_known_event_at,
        second_event_at_2,
        prev_event_at,
        prev_event_at_2,
        minutes_since_prev_event_at
    FROM sequenced
    WHERE event_at_rank_2 = 1
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(event_at_cume_dist_3) OVER (ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_at_cume_dist_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_at_rank ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        COUNT(*) OVER (PARTITION BY event_at_rank, TO_DATE(second_event_at_2)) AS rows_in_event_at_rank_second_event_at_2_to_date,
        ROUND(minutes_since_prev_event_at - AVG(minutes_since_prev_event_at) OVER (PARTITION BY event_at_rank, TO_DATE(second_event_at_2)), 2) AS minutes_since_prev_event_at_vs_event_at_rank_sec,
        LAG(prev_event_at) OVER w_all_by_event_at AS prev_prev_event_at,
        RANK() OVER (ORDER BY event_at, id) AS event_at_rank_2,
        ROUND(event_at_cume_dist_2 - AVG(event_at_cume_dist_2) OVER (PARTITION BY event_at_rank, TO_DATE(second_event_at_2)), 2) AS event_at_cume_dist_2_vs_event_at_rank_second_eve
    FROM top_rows
    WINDOW w_all_by_event_at AS (ORDER BY event_at, id), w_event_at_rank_second_event_a AS (PARTITION BY event_at_rank, TO_DATE(second_event_at_2))
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_clean_2,
    event_at_rank,
    prev2_event_at,
    event_at_cume_dist,
    first_event_at,
    event_at_cume_dist_2,
    event_at_cume_dist_3,
    second_event_at,
    last_known_event_at,
    second_event_at_2,
    prev_event_at,
    minutes_since_prev_event_at,
    running_event_at_cume_dist_3,
    event_at_percent_rank,
    rows_in_event_at_rank_second_event_at_2_to_date,
    prev_prev_event_at,
    event_at_cume_dist_2_vs_event_at_rank_second_eve,
    event_at_rank - event_at_cume_dist_2 AS event_at_rank_minus_event_at_cume_dist_2,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM sequenced_2
WHERE event_at_rank_2 <= 9
