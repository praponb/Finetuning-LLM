WITH prepared_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text,
        DATEDIFF(DATE '2025-12-31', opened_at) AS days_since_opened_at,
        GREATEST(requester_id, csat) AS greatest_requester_id_csat
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
      AND priority = 'P1'
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM prepared_tickets s
    WHERE NOT EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND su.tier IS NOT NULL
              AND YEAR(su.hired_on) = 2024
        )
),
latest AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        tags_text,
        days_since_opened_at,
        greatest_requester_id_csat
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id DESC) AS rn
        FROM qualified
    ) ranked
    WHERE rn = 1
),
exploded_detail_events AS (
    -- One row per event
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.tags_text,
        s.days_since_opened_at,
        s.greatest_requester_id_csat,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type,
        t.event_at
    FROM latest s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_opened_at / NULLIF(SUM(days_since_opened_at) OVER w_event_type, 0), 4) AS days_since_opened_at_share_of_event_type,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY priority ORDER BY days_since_opened_at ASC, id, event_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at,
        MIN(greatest_requester_id_csat) OVER (PARTITION BY event_type) AS event_type_min_greatest_requester_id_csat,
        ROUND(greatest_requester_id_csat / NULLIF(SUM(greatest_requester_id_csat) OVER (PARTITION BY event_type), 0), 4) AS greatest_requester_id_csat_share_of_event_type,
        COUNT(*) OVER (PARTITION BY event_type) AS rows_in_event_type
    FROM exploded_detail_events
    WINDOW w_event_type AS (PARTITION BY event_type)
)
-- Final projection
SELECT
    id,
    requester_id,
    opened_at,
    tags_text,
    greatest_requester_id_csat,
    event_id,
    event_actor,
    event_type,
    event_at,
    days_since_opened_at_share_of_event_type,
    second_event_at,
    greatest_requester_id_csat_share_of_event_type,
    rows_in_event_type,
    FLOOR(rows_in_event_type / 5) * 5 AS rows_in_event_type_bucket,
    LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
FROM metrics
ORDER BY event_actor DESC, priority_slug DESC
