WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 15 MINUTES AS event_at_plus_15m,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY event_at DESC) AS event_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_group ORDER BY event_at DESC, id), 3) AS event_at_percent_rank,
        RANK() OVER (PARTITION BY actor_group ORDER BY event_at DESC, id) AS event_at_rank,
        NTILE(5) OVER (ORDER BY event_at_plus_15m DESC, id) AS event_at_plus_15m_quintile,
        ROUND((UNIX_TIMESTAMP(event_at_plus_15m) - UNIX_TIMESTAMP(LAG(event_at_plus_15m) OVER (ORDER BY event_at_plus_15m DESC, id))) / 60, 1) AS minutes_since_prev_event_at_plus_15m,
        ROUND(CUME_DIST() OVER (ORDER BY event_at_plus_15m DESC, id), 3) AS event_at_plus_15m_cume_dist,
        LAST_VALUE(event_at) OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_event_at,
        NTILE(5) OVER w_actor_group_by_event_at AS event_at_quintile,
        NTH_VALUE(event_at, 2) OVER (ORDER BY event_at_plus_15m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        LAST_VALUE(event_at) IGNORE NULLS OVER (ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        DENSE_RANK() OVER (PARTITION BY actor_group ORDER BY event_at DESC) AS event_at_dense_rank_2
    FROM scoped_events
    WINDOW w_actor_group_by_event_at AS (PARTITION BY actor_group ORDER BY event_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_plus_15m,
        actor_group,
        event_at_dense_rank,
        event_at_percent_rank,
        event_at_rank,
        event_at_plus_15m_quintile,
        minutes_since_prev_event_at_plus_15m,
        event_at_plus_15m_cume_dist,
        last_event_at,
        event_at_quintile,
        second_event_at,
        last_known_event_at
    FROM sequenced
    WHERE event_at_dense_rank_2 = 1
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY event_at_quintile) AS rows_in_event_at_quintile,
        ROUND(PERCENT_RANK() OVER (ORDER BY event_at_plus_15m_quintile, id), 3) AS event_at_plus_15m_quintile_percent_rank,
        LAG(event_at, 3) OVER (ORDER BY event_at_plus_15m_quintile, id) AS prev3_event_at,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY event_at_quintile)) AS distinct_ticket_id_in_event_at_quintile,
        LEAD(event_at_plus_15m_quintile) OVER w_last_known_event_at_month_by AS next_event_at_plus_15m_quintile,
        MAX(event_at_dense_rank) OVER w_event_at_dense_rank AS event_at_dense_rank_max_event_at_dense_rank
    FROM top_rows
    WINDOW w_last_known_event_at_month_by AS (PARTITION BY DATE_TRUNC('MONTH', last_known_event_at) ORDER BY event_at_plus_15m DESC, id), w_event_at_dense_rank AS (PARTITION BY event_at_dense_rank)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_group,
    event_at_dense_rank,
    event_at_percent_rank,
    minutes_since_prev_event_at_plus_15m,
    last_event_at,
    event_at_quintile,
    second_event_at,
    rows_in_event_at_quintile,
    event_at_plus_15m_quintile_percent_rank,
    prev3_event_at,
    distinct_ticket_id_in_event_at_quintile,
    next_event_at_plus_15m_quintile,
    event_at_dense_rank_max_event_at_dense_rank,
    ABS(event_at_rank - 4) AS event_at_rank_distance,
    UPPER(TRIM(event_type)) AS event_type_clean
FROM sequenced_2
WHERE second_event_at < TIMESTAMP '2025-02-24 12:00:00'
ORDER BY id
