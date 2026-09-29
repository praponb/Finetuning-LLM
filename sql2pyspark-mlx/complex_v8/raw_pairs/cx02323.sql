WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE event_type = 'replied'
      AND actor = 'customer'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_type,
        event_at_hour,
        NTILE(4) OVER w_actor_ticket_id_by_event_at AS event_at_hour_quartile,
        ROW_NUMBER() OVER (PARTITION BY event_at_hour, event_type ORDER BY event_at_hour ASC, id) AS rn,
        NTH_VALUE(event_at_hour, 2) OVER (PARTITION BY event_type ORDER BY days_since_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_hour,
        NTILE(5) OVER w_event_type_by_days_since_eve_2 AS days_since_event_at_quintile,
        MIN(days_since_event_at) OVER (PARTITION BY actor, event_type) AS actor_event_type_min_days_since_event_at,
        MIN(days_since_event_at) OVER (PARTITION BY event_type) AS event_type_min_days_since_event_at,
        ROUND(AVG(event_at_hour) OVER (PARTITION BY event_at_hour, event_type ORDER BY event_at_hour ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS event_at_hour_avg_7,
        MAX(days_since_event_at) OVER w_event_type AS event_type_max_days_since_event_at,
        ROUND(event_at_hour - AVG(event_at_hour) OVER w_event_type, 2) AS event_at_hour_vs_event_type_avg,
        FIRST_VALUE(event_at_hour) IGNORE NULLS OVER (PARTITION BY event_at_hour, event_type ORDER BY days_since_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at_hour,
        LAST_VALUE(event_at_hour) IGNORE NULLS OVER (PARTITION BY event_at_hour, event_type ORDER BY days_since_event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_at_hour
    FROM prepared
    WINDOW w_actor_ticket_id_by_event_at AS (PARTITION BY actor, ticket_id ORDER BY event_at_hour NULLS LAST, id), w_event_type_by_days_since_eve_2 AS (PARTITION BY event_type ORDER BY days_since_event_at DESC, id), w_event_type AS (PARTITION BY event_type)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at_hour,
        event_at_hour_quartile,
        rn,
        actor_event_type_min_days_since_event_at,
        event_type_min_days_since_event_at,
        event_at_hour_avg_7,
        event_type_max_days_since_event_at,
        event_at_hour_vs_event_type_avg,
        first_known_event_at_hour,
        last_known_event_at_hour,
        LEAST(second_event_at_hour, event_at_hour) AS least_second_event_at_hour_event_at_hour,
        ABS(event_at_hour - 12) AS event_at_hour_distance
    FROM windowed
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY last_known_event_at_hour, ticket_id) AS rows_in_last_known_event_at_hour_ticket_id,
        COUNT(*) OVER (PARTITION BY least_second_event_at_hour_event_at_hour, event_at_hour) AS rows_in_least_second_event_at_hour_event_at_hour,
        LEAD(event_at_hour, 1, 0) OVER (PARTITION BY first_known_event_at_hour, last_known_event_at_hour ORDER BY first_known_event_at_hour DESC, id) AS next_event_at_hour,
        SUM(event_at_hour) OVER (PARTITION BY rn ORDER BY least_second_event_at_hour_event_at_hour, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_at_hour_sum_4,
        ROUND(event_at_hour / NULLIF(SUM(event_at_hour) OVER (PARTITION BY least_second_event_at_hour_event_at_hour, event_at_hour), 0), 4) AS event_at_hour_share_of_least_second_event_at_hou,
        DENSE_RANK() OVER (PARTITION BY first_known_event_at_hour, last_known_event_at_hour ORDER BY first_known_event_at_hour DESC) AS first_known_event_at_hour_dense_rank,
        MIN(event_at_hour) OVER (PARTITION BY least_second_event_at_hour_event_at_hour, event_at_hour) AS least_second_event_at_hour_event_at_hour_event_a,
        NTH_VALUE(event_at_hour, 2) OVER (PARTITION BY last_known_event_at_hour, event_at_hour_quartile ORDER BY least_second_event_at_hour_event_at_hour NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at_hour
    FROM annotated
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at_hour_quartile,
    actor_event_type_min_days_since_event_at,
    event_type_min_days_since_event_at,
    event_at_hour_avg_7,
    event_at_hour_vs_event_type_avg,
    first_known_event_at_hour,
    last_known_event_at_hour,
    least_second_event_at_hour_event_at_hour,
    event_at_hour_distance,
    rows_in_last_known_event_at_hour_ticket_id,
    rows_in_least_second_event_at_hour_event_at_hour,
    next_event_at_hour,
    event_at_hour_share_of_least_second_event_at_hou,
    least_second_event_at_hour_event_at_hour_event_a,
    second_event_at_hour,
    LEAST(next_event_at_hour, second_event_at_hour) AS least_next_event_at_hour_second_event_at_hour,
    COALESCE(second_event_at_hour, 0) AS second_event_at_hour_filled
FROM metrics
ORDER BY rows_in_last_known_event_at_hour_ticket_id NULLS LAST, least_second_event_at_hour_event_at_hour DESC, id
LIMIT 10
