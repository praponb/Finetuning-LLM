WITH recent AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown,
        COALESCE(requester_id, 0) AS requester_id_filled,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM tickets
    WHERE (priority NOT IN ('P1', 'P4') OR opened_at >= TIMESTAMP '2025-01-25 11:00:00')
),
exploded AS (
    -- One row per tag
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.priority_or_unknown,
        s.requester_id_filled,
        s.priority_prefix,
        t.tag
    FROM recent s
    LATERAL VIEW EXPLODE(s.tags) t AS tag
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(tag), ' ', '_')) AS tag_slug,
        LENGTH(TRIM(priority)) AS priority_len
    FROM exploded
),
tagged_2 AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN priority_len >= 10 THEN 'A' WHEN priority_len >= 6 THEN 'B' ELSE 'C' END AS priority_len_tier,
        CONCAT_WS('|', tag, priority_or_unknown) AS tag_priority_or_unknown_label,
        ROUND((UNIX_TIMESTAMP(closed_at) - UNIX_TIMESTAMP(opened_at)) / 3600, 1) AS hours_opened_at_to_closed_at
    FROM tagged
)
SELECT
    priority,
    agent_id,
    priority_or_unknown,
    ROUND(SUM(requester_id_filled) / NULLIF(SUM(hours_opened_at_to_closed_at), 0), 3) AS requester_id_filled_to_hours_opened_at_to_closed
FROM tagged_2
GROUP BY ROLLUP(priority, agent_id, priority_or_unknown)
ORDER BY priority NULLS LAST, agent_id NULLS LAST, priority_or_unknown NULLS LAST
