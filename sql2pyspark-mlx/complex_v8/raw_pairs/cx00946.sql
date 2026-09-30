WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at_2
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
latest AS (
    -- Keep the earliest row per event_type, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at,
        event_type_actor_label,
        days_since_event_at_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type, ticket_id ORDER BY days_since_event_at_2, id) AS recency_rank
        FROM recent
    ) ranked
    WHERE recency_rank = 1
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_type_actor_label,
        s.days_since_event_at_2,
        t.tags AS ticket_tags
    FROM latest s
    JOIN tickets t ON t.id = s.ticket_id
),
actor_stats AS (
    -- Median days_since_event_at per actor
    SELECT actor, PERCENTILE_APPROX(days_since_event_at, 0.5) AS median_days_since_event_at
    FROM joined
    GROUP BY actor
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_days_since_event_at,
        ROUND(s.days_since_event_at - g.median_days_since_event_at, 2) AS days_since_event_at_minus_median
    FROM joined s
    JOIN actor_stats g ON g.actor = s.actor
    WHERE s.days_since_event_at >= g.median_days_since_event_at
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        ROUND(days_since_event_at_2 / NULLIF(days_since_event_at, 0), 3) AS days_since_event_at_2_per_days_since_event_at
    FROM outliers
),
exploded AS (
    -- One row per ticket_tag
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        s.days_since_event_at,
        s.event_type_actor_label,
        s.days_since_event_at_2,
        s.median_days_since_event_at,
        s.days_since_event_at_minus_median,
        s.event_type_ref,
        s.days_since_event_at_2_per_days_since_event_at,
        t.ticket_tag
    FROM enriched s
    LATERAL VIEW OUTER EXPLODE(s.ticket_tags) t AS ticket_tag
),
labeled AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        event_type_actor_label,
        days_since_event_at_2,
        median_days_since_event_at,
        ticket_tag,
        FLOOR(days_since_event_at / 100) * 100 AS days_since_event_at_bucket,
        COALESCE(days_since_event_at_2, 0) AS days_since_event_at_2_filled
    FROM exploded
    WHERE ticket_tag IN ('refund', 'upgrade')
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_type_actor_label,
    days_since_event_at_2,
    median_days_since_event_at,
    ticket_tag,
    days_since_event_at_bucket,
    days_since_event_at_2_filled,
    COALESCE(ticket_tag, 'UNKNOWN') AS ticket_tag_or_unknown
FROM labeled
ORDER BY event_type, actor DESC, id, ticket_tag
LIMIT 5
