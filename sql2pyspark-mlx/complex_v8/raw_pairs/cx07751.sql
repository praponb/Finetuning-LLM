WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-18'
      AND actor NOT IN ('system', 'agent')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', event_at), ticket_id ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        NTILE(4) OVER w_ticket_id_actor_by_event_at_2 AS event_at_quartile,
        ROW_NUMBER() OVER w_event_at_month_ticket_id_by AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at), 3) AS event_at_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_dense_rank,
        LAG(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS prev2_event_at,
        LAG(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS prev_event_at,
        DENSE_RANK() OVER (PARTITION BY actor ORDER BY event_at) AS event_at_dense_rank_2,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at, id), 3) AS event_at_cume_dist_3,
        ROW_NUMBER() OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id) AS event_at_seq,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type ORDER BY event_at, id), 3) AS event_at_percent_rank,
        RANK() OVER (PARTITION BY event_type ORDER BY event_at, id) AS event_at_rank,
        FIRST_VALUE(event_at) OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_event_at
    FROM scoped
    WINDOW w_ticket_id_actor_by_event_at_2 AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id), w_event_at_month_ticket_id_by AS (PARTITION BY DATE_TRUNC('MONTH', event_at), ticket_id ORDER BY event_at DESC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        second_event_at,
        event_at_cume_dist,
        event_at_quartile,
        seq_no,
        event_at_cume_dist_2,
        event_at_dense_rank_2,
        last_known_event_at,
        event_at_cume_dist_3,
        event_at_seq,
        event_at_percent_rank,
        LEAST(event_at_cume_dist_3, event_at_cume_dist) AS least_event_at_cume_dist_3_event_at_cume_dist,
        ROUND(event_at_cume_dist_3 * 1.1, 2) AS event_at_cume_dist_3_adj
    FROM metrics
    WHERE event_type = 'escalated'
)
SELECT
    id,
    event_type,
    actor,
    second_event_at,
    event_at_quartile,
    seq_no,
    event_at_cume_dist_2,
    event_at_dense_rank_2,
    last_known_event_at,
    event_at_cume_dist_3,
    event_at_seq,
    event_at_percent_rank
FROM scored
WHERE event_at_percent_rank > 0.2
ORDER BY event_at_cume_dist_2 NULLS LAST, event_at_seq DESC
