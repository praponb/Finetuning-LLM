WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_slug,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY ticket_id, actor ORDER BY event_at NULLS LAST), 3) AS event_at_percent_rank
    FROM base_events
),
latest_per_key AS (
    -- Keep the latest row per ticket_id, event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_slug,
        second_event_at,
        event_at_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id, event_type ORDER BY event_at_percent_rank DESC, id DESC) AS rn
        FROM with_windows
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        second_event_at,
        event_at_percent_rank,
        ROUND(AVG(event_at_percent_rank) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS event_at_percent_rank_avg_3,
        MIN(event_at_percent_rank) OVER w_event_type AS event_type_min_event_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(second_event_at) - UNIX_TIMESTAMP(LAG(second_event_at) OVER (ORDER BY event_at DESC, id))) / 60, 1) AS minutes_since_prev_second_event_at,
        COUNT(*) OVER w_event_type AS rows_in_event_type,
        MAX(event_at_percent_rank) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS event_at_percent_rank_max_1_prior
    FROM latest_per_key
    WINDOW w_event_type AS (PARTITION BY event_type)
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    second_event_at,
    event_at_percent_rank,
    event_at_percent_rank_avg_3,
    event_type_min_event_at_percent_rank,
    minutes_since_prev_second_event_at,
    rows_in_event_type,
    event_at_percent_rank_max_1_prior,
    LEAST(rows_in_event_type, event_at_percent_rank) AS least_rows_in_event_type_event_at_percent_rank
FROM sequenced
ORDER BY rows_in_event_type DESC, id
LIMIT 5
