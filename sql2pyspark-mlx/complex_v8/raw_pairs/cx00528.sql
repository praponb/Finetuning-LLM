WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 11:00:00'
),
latest AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at_dayofweek
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at DESC, id DESC) AS rn
        FROM prepared_events
    ) ranked
    WHERE rn = 1
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN event_type IN ('reopened', 'replied') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM latest
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        event_type,
        event_type_group,
        CONCAT_WS(' / ', event_type_group, actor) AS event_type_group_actor_label,
        LENGTH(TRIM(actor)) AS actor_len
    FROM tagged
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_type,
        event_type_group,
        actor_len,
        SUM(actor_len) OVER (PARTITION BY actor_len ORDER BY actor_len ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS actor_len_sum_1_prior,
        FIRST_VALUE(actor_len) IGNORE NULLS OVER (PARTITION BY actor_len ORDER BY actor_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_actor_len,
        ROUND(CUME_DIST() OVER (PARTITION BY actor_len ORDER BY actor_len ASC), 3) AS actor_len_cume_dist,
        MAX(actor_len) OVER (PARTITION BY event_type_group) AS event_type_group_max_actor_len
    FROM annotated
),
latest_per_key AS (
    -- Keep the latest row per event_type_group, event_type
    SELECT
        id,
        event_type,
        event_type_group,
        actor_len,
        actor_len_sum_1_prior,
        first_known_actor_len,
        actor_len_cume_dist,
        event_type_group_max_actor_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type_group, event_type ORDER BY actor_len_cume_dist DESC, id DESC) AS row_num
        FROM ranked
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    actor_len,
    actor_len_sum_1_prior,
    first_known_actor_len,
    actor_len_cume_dist,
    event_type_group_max_actor_len,
    ROUND(first_known_actor_len / NULLIF(actor_len_cume_dist, 0), 3) AS first_known_actor_len_per_actor_len_cume_dist,
    ABS(first_known_actor_len - 10) AS first_known_actor_len_distance
FROM latest_per_key
