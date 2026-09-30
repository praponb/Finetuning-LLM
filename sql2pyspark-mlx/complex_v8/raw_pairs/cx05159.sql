WITH base_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 60, 1) AS minutes_opened_at_to_closed_at,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE agent_id IS NOT NULL
),
unique_rows AS (
    -- Keep the earliest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        tags,
        minutes_opened_at_to_closed_at,
        tags_count
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY requester_id, id) AS row_num
        FROM base_tickets
    ) ranked
    WHERE row_num = 1
),
qualified AS (
    -- Keep rows whose agent matches
    SELECT s.*
    FROM unique_rows s
    WHERE EXISTS (
            SELECT 1
            FROM support_agents su
            WHERE su.id = s.agent_id
              AND UPPER(TRIM(su.agent_name)) LIKE 'G%'
              AND su.tier <> 'T1'
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        closed_at,
        priority,
        tags,
        tags_count,
        COALESCE(tags_count, 0) AS tags_count_filled,
        CASE WHEN priority IN ('P4', 'P3') THEN 'group_a' WHEN priority = 'P2' THEN 'group_b' ELSE 'other' END AS priority_group,
        DATE_FORMAT(closed_at, 'yyyy-MM-dd') AS closed_at_label
    FROM qualified
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        priority,
        tags_count,
        tags_count_filled,
        priority_group,
        closed_at_label,
        CASE WHEN tags_count_filled >= 2 THEN 'large' WHEN tags_count_filled >= 1 THEN 'medium' ELSE 'small' END AS tags_count_filled_tier
    FROM enriched
    WHERE priority_group = 'group_a'
),
screened AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.id
              AND t.event_at < TIMESTAMP '2025-02-18 12:00:00'
        )
      AND s.tags_count < 2
)
-- Final projection
SELECT
    id,
    priority,
    tags_count,
    tags_count_filled,
    priority_group,
    closed_at_label,
    CASE WHEN tags_count_filled_tier IN ('medium', 'large') THEN 'group_a' WHEN tags_count_filled_tier = 'small' THEN 'group_b' ELSE 'other' END AS tags_count_filled_tier_group
FROM screened
ORDER BY tags_count_filled
