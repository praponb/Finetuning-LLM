WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
    WHERE ticket_id <= 13
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(days_since_event_at / 100) * 100 AS days_since_event_at_bucket,
        DATE_TRUNC('YEAR', event_at) AS event_at_year
    FROM base
    WHERE UPPER(TRIM(actor_clean)) LIKE 'S%'
),
latest AS (
    -- Keep the latest row per event_type, actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_slug,
        days_since_event_at,
        actor_clean,
        days_since_event_at_bucket,
        event_at_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, actor ORDER BY days_since_event_at DESC, id DESC) AS row_num
        FROM annotated
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER w_event_at_year_year_actor, 0), 4) AS days_since_event_at_share_of_event_at_year_year_,
        SUM(days_since_event_at) OVER (PARTITION BY MONTH(event_at_year) ORDER BY event_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS days_since_event_at_sum_2,
        ROUND(AVG(days_since_event_at_bucket) OVER w_event_at_year_year_actor, 2) AS event_at_year_year_actor_avg_days_since_event_at,
        ROUND(AVG(days_since_event_at_bucket) OVER (PARTITION BY MONTH(event_at_year) ORDER BY event_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS days_since_event_at_bucket_avg_7,
        COUNT(*) OVER (PARTITION BY YEAR(event_at_year), actor) AS rows_in_event_at_year_year_actor
    FROM latest
    WINDOW w_event_at_year_year_actor AS (PARTITION BY YEAR(event_at_year), actor)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_clean,
        event_at_year,
        days_since_event_at_share_of_event_at_year_year_,
        days_since_event_at_sum_2,
        event_at_year_year_actor_avg_days_since_event_at,
        days_since_event_at_bucket_avg_7,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(event_at_year)) / 60, 1) AS minutes_event_at_year_to_event_at,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM windowed
),
deduped AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        actor_clean,
        event_at_year,
        days_since_event_at_share_of_event_at_year_year_,
        days_since_event_at_sum_2,
        event_at_year_year_actor_avg_days_since_event_at,
        days_since_event_at_bucket_avg_7,
        minutes_event_at_year_to_event_at,
        actor_group
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at_year DESC, id DESC) AS recency_rank
        FROM tagged
    ) ranked
    WHERE recency_rank = 1
),
grouped AS (
    -- Summarise per group
    SELECT
        event_type,
        ticket_id,
        actor,
        SUM(minutes_event_at_year_to_event_at) FILTER (WHERE actor IN ('system', 'agent')) AS minutes_event_at_year_to_event_at_actor_in_syste,
        ROUND(SUM(days_since_event_at_share_of_event_at_year_year_) / NULLIF(SUM(days_since_event_at_bucket_avg_7), 0), 3) AS days_since_event_at_share_of_event_at_year_year__2,
        SUM(minutes_event_at_year_to_event_at) AS total_minutes_event_at_year_to_event_at,
        ROUND(STDDEV(days_since_event_at_bucket_avg_7), 2) AS stddev_days_since_event_at_bucket_avg_7
    FROM deduped
    WHERE days_since_event_at_bucket_avg_7 < 525.0
    GROUP BY event_type, ticket_id, actor
)
-- Final projection
SELECT
    event_type,
    ticket_id,
    actor,
    minutes_event_at_year_to_event_at_actor_in_syste,
    total_minutes_event_at_year_to_event_at
FROM grouped
ORDER BY minutes_event_at_year_to_event_at_actor_in_syste DESC NULLS FIRST, ticket_id DESC, event_type, actor
LIMIT 20
