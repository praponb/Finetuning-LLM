WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC), 3) AS event_at_percent_rank,
        LAG(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id) AS prev_event_at,
        NTILE(4) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS event_at_quartile,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY event_type ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROW_NUMBER() OVER w_actor_ticket_id_by_event_at AS rn,
        NTILE(5) OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id) AS event_at_quintile,
        LAST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at,
        LEAD(event_at) OVER w_event_type_by_event_at AS next_event_at,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (PARTITION BY TO_DATE(event_at) ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM prepared_events
    WINDOW w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id), w_event_type_by_event_at AS (PARTITION BY event_type ORDER BY event_at NULLS LAST, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        first_known_event_at,
        event_at_percent_rank,
        event_at_quartile,
        second_event_at,
        event_at_quintile,
        next_event_at,
        minutes_since_prev_event_at,
        event_at_quartile - LAG(event_at_quartile) OVER w_rn_by_event_at_quartile AS event_at_quartile_change,
        ROUND(rn - AVG(rn) OVER (PARTITION BY event_type), 2) AS rn_vs_event_type_avg,
        DENSE_RANK() OVER w_all_by_last_known_event_at_t AS last_known_event_at_dense_rank,
        MAX(event_at_quintile) OVER (PARTITION BY rn ORDER BY event_at_quartile, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_at_quintile_max_3_prior,
        ROUND(AVG(event_at_quartile) OVER (PARTITION BY event_type), 2) AS event_type_avg_event_at_quartile,
        NTH_VALUE(event_at_percent_rank, 2) OVER (PARTITION BY actor, rn ORDER BY event_at_percent_rank DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_percent_rank,
        ROUND(minutes_since_prev_event_at - AVG(minutes_since_prev_event_at) OVER w_event_type, 2) AS minutes_since_prev_event_at_vs_event_type_avg
    FROM sequenced
    WINDOW w_rn_by_event_at_quartile AS (PARTITION BY rn ORDER BY event_at_quartile, id), w_all_by_last_known_event_at_t AS (ORDER BY last_known_event_at), w_event_type AS (PARTITION BY event_type)
)
SELECT
    id,
    event_type,
    actor,
    first_known_event_at,
    event_at_quartile,
    second_event_at,
    minutes_since_prev_event_at,
    event_at_quartile_change,
    rn_vs_event_type_avg,
    last_known_event_at_dense_rank,
    event_at_quintile_max_3_prior,
    event_type_avg_event_at_quartile,
    second_event_at_percent_rank,
    minutes_since_prev_event_at_vs_event_type_avg,
    ROUND(rn_vs_event_type_avg * 1.5, 2) AS rn_vs_event_type_avg_adj,
    COALESCE(event_at_percent_rank, 0.0) AS event_at_percent_rank_filled
FROM ranked
ORDER BY actor
