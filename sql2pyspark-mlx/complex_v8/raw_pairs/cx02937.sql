WITH base_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority
    FROM tickets
    WHERE csat >= 2
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix,
        SUBSTRING(TRIM(priority), 1, 3) AS priority_prefix_2
    FROM base_tickets
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(requester_id) OVER (PARTITION BY agent_id, YEAR(closed_at)) AS agent_id_closed_at_year_max_requester_id,
        SUM(requester_id) OVER (PARTITION BY YEAR(closed_at) ORDER BY requester_id NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_requester_id,
        LAG(requester_id, 1, 0) OVER (PARTITION BY agent_id, priority ORDER BY closed_at, id) AS prev_requester_id
    FROM enriched
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM sequenced
    WHERE requester_id < (SELECT MAX(requester_id) FROM sequenced)
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', priority, priority_slug) AS priority_priority_slug_label,
        DATE_TRUNC('WEEK', closed_at) AS closed_at_week,
        COALESCE(prev_requester_id, 0) AS prev_requester_id_filled
    FROM with_activity
)
SELECT
    id,
    closed_at,
    priority_slug,
    priority_prefix,
    priority_prefix_2,
    prev_requester_id,
    priority_priority_slug_label,
    closed_at_week,
    prev_requester_id_filled,
    CASE WHEN priority IN ('P3', 'P1') THEN 'group_a' WHEN priority = 'P4' THEN 'group_b' ELSE 'other' END AS priority_group,
    FLOOR(requester_id / 5) * 5 AS requester_id_bucket
FROM derived
ORDER BY prev_requester_id_filled DESC NULLS FIRST
