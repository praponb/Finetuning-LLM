WITH cleaned_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM ticket_events
    WHERE ticket_id >= 30
      AND actor NOT IN ('system', 'agent')
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        event_type_ref,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label
    FROM cleaned_events
    WHERE ticket_id >= 38
),
latest AS (
    -- Keep the latest row per event_type
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        actor_event_type_label,
        event_type_ref,
        event_at_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_at DESC, id DESC) AS row_num
        FROM annotated
    ) ranked
    WHERE row_num = 1
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        actor_event_type_label,
        event_type_ref,
        event_at_label,
        RANK() OVER w_event_at_month_actor_by_even AS event_at_rank,
        LEAD(event_at, 3) OVER (PARTITION BY MONTH(event_at), actor ORDER BY event_at, id) AS next3_event_at
    FROM latest
    WINDOW w_event_at_month_actor_by_even AS (PARTITION BY MONTH(event_at), actor ORDER BY event_at)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM windowed
    WHERE event_at_rank < (SELECT MAX(event_at_rank) FROM windowed)
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(CASE WHEN event_type NOT IN ('reopened', 'escalated') THEN 1 END) AS event_event_type_count,
        COUNT(CASE WHEN event_type IN ('reopened', 'created') THEN 1 END) AS event_event_type_count_2
    FROM ticket_events
    GROUP BY ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_event_type_count, 0) AS event_event_type_count,
        COALESCE(t.event_event_type_count_2, 0) AS event_event_type_count_2
    FROM with_activity s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        event_at_rank - event_event_type_count AS event_at_rank_minus_event_event_type_count,
        DATE_FORMAT(next3_event_at, 'yyyy-MM') AS next3_event_at_label,
        SUBSTRING(TRIM(actor_event_type_label), 1, 4) AS actor_event_type_label_prefix
    FROM with_counts
    WHERE event_at_label IS NOT NULL
)
SELECT
    id,
    ticket_id,
    event_at,
    actor_event_type_label,
    event_type_ref,
    event_at_label,
    event_event_type_count,
    next3_event_at_label,
    actor_event_type_label_prefix,
    CONCAT_WS(': ', next3_event_at_label, actor_event_type_label) AS next3_event_at_label_actor_event_type_label_labe,
    ABS(event_event_type_count_2 - 4) AS event_event_type_count_2_distance
FROM scored
WHERE TO_DATE(next3_event_at) < DATE '2025-03-14'
ORDER BY event_at, actor_event_type_label NULLS LAST, id
LIMIT 10
