WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE event_type = 'resolved'
      AND HOUR(event_at) BETWEEN 6 AND 10
),
latest_per_key AS (
    -- Keep the latest row per actor
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        days_since_event_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor ORDER BY event_at DESC, id DESC) AS rn
        FROM recent
    ) ranked
    WHERE rn = 1
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.last_event_at_2
    FROM latest_per_key s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_event_at / NULLIF(SUM(days_since_event_at) OVER (PARTITION BY MONTH(last_event_at), ticket_id), 0), 4) AS days_since_event_at_share_of_last_event_at_month,
        ROUND(CUME_DIST() OVER w_actor_event_type_by_event_at, 3) AS event_at_cume_dist
    FROM with_counts
    WINDOW w_actor_event_type_by_event_at AS (PARTITION BY actor, event_type ORDER BY event_at ASC, id)
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
        )
      AND s.event_type = 'created'
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.days_since_event_at,
        s.last_event_at,
        s.last_event_at_2,
        s.days_since_event_at_share_of_last_event_at_month,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM with_activity s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DATE_FORMAT(last_event_at, 'yyyy-MM-dd') AS last_event_at_label,
        MONTH(last_event_at) AS last_event_at_month
    FROM line_level_events
)
SELECT
    id,
    last_event_at,
    event_id,
    event_event_at,
    event_event_type,
    last_event_at_label,
    last_event_at_month,
    CASE WHEN days_since_event_at >= 455 THEN 'HIGH' WHEN days_since_event_at >= 245 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_event_at_tier
FROM enriched
WHERE last_event_at_label IS NOT NULL
