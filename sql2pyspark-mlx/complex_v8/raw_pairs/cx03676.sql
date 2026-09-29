WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE ticket_id <= 53
      AND actor IN ('system', 'customer')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        actor,
        LEAD(event_at, 3) OVER (PARTITION BY TO_DATE(event_at), actor_len ORDER BY actor_len DESC, id) AS next3_event_at,
        SUM(actor_len) OVER w_ticket_id AS ticket_id_sum_actor_len,
        ROUND(actor_len / NULLIF(SUM(actor_len) OVER (PARTITION BY ticket_id), 0), 4) AS actor_len_share_of_ticket_id,
        SUM(actor_len) OVER (PARTITION BY TO_DATE(event_at), actor_len ORDER BY actor_len DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS actor_len_sum_7,
        DENSE_RANK() OVER (PARTITION BY actor, YEAR(event_at) ORDER BY actor_len DESC, id) AS actor_len_dense_rank,
        RANK() OVER (ORDER BY actor_len DESC, id) AS actor_len_rank,
        NTH_VALUE(actor_len, 2) OVER (PARTITION BY actor_len, ticket_id ORDER BY actor_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_actor_len,
        RANK() OVER (ORDER BY event_at DESC NULLS FIRST, id) AS event_at_rank,
        ROUND(AVG(actor_len) OVER w_ticket_id_actor_len, 2) AS ticket_id_actor_len_avg_actor_len,
        ROUND(PERCENT_RANK() OVER (PARTITION BY actor_len, ticket_id ORDER BY actor_len DESC), 3) AS actor_len_percent_rank,
        FIRST_VALUE(actor_len) OVER (ORDER BY actor_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_actor_len,
        MAX(actor_len) OVER (PARTITION BY actor, event_type) AS actor_event_type_max_actor_len,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type, actor ORDER BY actor_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        SIZE(COLLECT_SET(ticket_id) OVER (PARTITION BY actor, event_type)) AS distinct_ticket_id_in_actor_event_type
    FROM scoped
    WINDOW w_ticket_id AS (PARTITION BY ticket_id), w_ticket_id_actor_len AS (PARTITION BY ticket_id, actor_len)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        distinct_ticket_id_in_actor_event_type,
        event_at_rank,
        ROUND(SUM(actor_len_dense_rank) / NULLIF(SUM(first_actor_len), 0), 3) AS actor_len_dense_rank_to_first_actor_len_ratio,
        COUNT(DISTINCT actor) AS distinct_actors,
        MIN(next3_event_at) AS first_next3_event_at
    FROM sequenced
    GROUP BY 1, 2
)
-- Final projection
SELECT
    distinct_ticket_id_in_actor_event_type,
    event_at_rank,
    actor_len_dense_rank_to_first_actor_len_ratio,
    distinct_actors,
    first_next3_event_at
FROM grouped
ORDER BY distinct_ticket_id_in_actor_event_type DESC NULLS FIRST, event_at_rank
