WITH valid AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug
    FROM tickets
    WHERE channel IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        opened_at + INTERVAL 15 MINUTES AS opened_at_plus_15m,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
        LENGTH(TRIM(priority)) AS priority_len
    FROM valid
),
priority_stats AS (
    -- Max csat per priority
    SELECT priority, MAX(csat) AS max_csat
    FROM annotated
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM annotated s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.csat = g.max_csat
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        opened_at,
        priority,
        csat,
        tags,
        max_csat,
        csat_minus_max,
        ROUND(csat_minus_max / NULLIF(requester_id, 0), 3) AS csat_minus_max_per_requester_id,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
        COALESCE(priority_len, 0) AS priority_len_filled
    FROM vs_group
    WHERE HOUR(opened_at_plus_15m) BETWEEN 10 AND 13
),
enriched_2 AS (
    -- Label and score rows
    SELECT
        id,
        priority,
        tags,
        max_csat,
        csat_minus_max,
        csat_minus_max_per_requester_id,
        priority_len_filled,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        DATE_FORMAT(opened_at, 'MMM yyyy') AS opened_at_label
    FROM enriched
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.priority,
        s.tags,
        s.priority_len_filled,
        s.priority_slug,
        s.opened_at_label,
        t.id AS event_id,
        t.event_type,
        t.event_at,
        t.actor AS event_actor
    FROM enriched_2 s
    JOIN ticket_events t ON t.ticket_id = s.id
)
SELECT
    id,
    priority_len_filled,
    priority_slug,
    opened_at_label,
    event_at,
    event_actor,
    MONTH(event_at) AS event_at_month,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
FROM line_level_events
WHERE priority IS NOT NULL
ORDER BY event_at, priority_len_filled
