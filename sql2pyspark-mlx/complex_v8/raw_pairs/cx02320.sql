WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        HOUR(event_at) AS event_at_hour,
        SUBSTRING(TRIM(event_type), 1, 2) AS event_type_prefix
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type = 'resolved'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        event_at,
        event_type,
        event_at_hour,
        DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_at_dayofweek DESC) AS event_at_dayofweek_dense_rank,
        SIZE(COLLECT_SET(actor) OVER (PARTITION BY ticket_id)) AS distinct_actor_in_ticket_id,
        SUM(event_at_hour) OVER (PARTITION BY event_type ORDER BY event_at_dayofweek DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS event_at_hour_sum_6_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY event_type ORDER BY event_at_dayofweek DESC, id), 3) AS event_at_dayofweek_cume_dist
    FROM recent_events
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        event_at,
        event_type,
        event_at_hour,
        distinct_actor_in_ticket_id,
        event_at_hour_sum_6_prior,
        event_at_dayofweek_cume_dist
    FROM ranked
    WHERE event_at_dayofweek_dense_rank <= 5
      AND event_at_hour >= 8
),
deduped AS (
    -- Keep the earliest row per event_type
    SELECT
        id,
        event_at,
        event_type,
        event_at_hour,
        distinct_actor_in_ticket_id,
        event_at_hour_sum_6_prior,
        event_at_dayofweek_cume_dist
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at_hour_sum_6_prior, id) AS rn
        FROM top_per_group
    ) ranked
    WHERE rn = 1
),
per_group AS (
    -- Summarise per group
    SELECT event_type, COUNT(*) AS n_rows, COUNT(DISTINCT id) AS distinct_ids
    FROM deduped
    GROUP BY event_type
)
SELECT
    event_type,
    n_rows,
    distinct_ids,
    FLOOR(n_rows / 2) * 2 AS n_rows_bucket
FROM per_group
ORDER BY event_type DESC
