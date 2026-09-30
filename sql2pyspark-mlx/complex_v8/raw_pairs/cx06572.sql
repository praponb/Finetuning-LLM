WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 10:00:00'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        LEAD(event_at) OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id))) / 60, 1) AS minutes_since_prev_event_at,
        ROW_NUMBER() OVER (ORDER BY event_at DESC, id) AS rn,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at DESC, id) AS event_at_seq,
        DENSE_RANK() OVER w_ticket_id_by_event_at AS event_at_dense_rank,
        ROUND(CUME_DIST() OVER (ORDER BY event_at), 3) AS event_at_cume_dist,
        ROW_NUMBER() OVER w_ticket_id_by_event_at AS rn_2,
        NTILE(5) OVER w_all_by_event_at AS event_at_quintile,
        LEAD(event_at) OVER w_ticket_id_by_event_at AS next_event_at_2,
        NTILE(5) OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_quintile_2,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER w_ticket_id_by_event_at)) / 60, 1) AS minutes_since_prev_event_at_2,
        LEAD(event_at) OVER (PARTITION BY event_type ORDER BY event_at, id) AS next_event_at_3,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_rank
    FROM filtered
    WINDOW w_ticket_id_by_event_at AS (PARTITION BY ticket_id ORDER BY event_at NULLS LAST, id), w_all_by_event_at AS (ORDER BY event_at DESC, id)
),
top_rows AS (
    -- First row per group
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        next_event_at,
        minutes_since_prev_event_at,
        event_at_seq,
        event_at_dense_rank,
        event_at_cume_dist,
        rn_2,
        event_at_quintile,
        next_event_at_2,
        event_at_quintile_2,
        minutes_since_prev_event_at_2,
        next_event_at_3,
        event_at_rank
    FROM ranked
    WHERE rn = 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(event_at_cume_dist) OVER (PARTITION BY event_at_quintile) AS event_at_quintile_sum_event_at_cume_dist,
        ROUND(event_at_quintile_2 / NULLIF(SUM(event_at_quintile_2) OVER (PARTITION BY event_at_quintile), 0), 4) AS event_at_quintile_2_share_of_event_at_quintile,
        ROUND(AVG(event_at_rank) OVER (PARTITION BY actor, event_at_dense_rank ORDER BY event_at_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS event_at_rank_avg_7,
        SUM(event_at_dense_rank) OVER w_event_at_quintile AS event_at_quintile_sum_event_at_dense_rank,
        ROUND(rn_2 - AVG(rn_2) OVER (PARTITION BY event_at_quintile), 2) AS rn_2_vs_event_at_quintile_avg
    FROM top_rows
    WINDOW w_event_at_quintile AS (PARTITION BY event_at_quintile)
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    next_event_at,
    minutes_since_prev_event_at,
    event_at_seq,
    event_at_dense_rank,
    event_at_cume_dist,
    event_at_quintile,
    next_event_at_2,
    event_at_quintile_2,
    minutes_since_prev_event_at_2,
    next_event_at_3,
    event_at_rank,
    event_at_quintile_sum_event_at_cume_dist,
    event_at_quintile_2_share_of_event_at_quintile,
    event_at_rank_avg_7,
    event_at_quintile_sum_event_at_dense_rank,
    rn_2_vs_event_at_quintile_avg,
    CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group,
    ROUND(event_at_rank / NULLIF(event_at_quintile_2, 0), 3) AS event_at_rank_per_event_at_quintile_2
FROM windowed
ORDER BY event_at_quintile_2, event_at_rank_per_event_at_quintile_2 DESC
