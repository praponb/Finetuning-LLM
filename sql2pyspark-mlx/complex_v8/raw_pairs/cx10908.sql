WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        requester_id - csat AS requester_id_minus_csat,
        CASE WHEN priority IN ('P3', 'P4') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE csat > 1
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel <> 'phone'
              AND t.csat <= 5
        )
),
unique_rows AS (
    -- Keep the latest row per priority_group
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        requester_id_minus_csat,
        priority_group,
        days_since_opened_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority_group ORDER BY requester_id_minus_csat DESC, id DESC) AS row_num
        FROM without_matches
    ) ranked
    WHERE row_num = 1
),
priority_group_stats AS (
    -- Max csat per priority_group
    SELECT priority_group, MAX(csat) AS max_csat
    FROM unique_rows
    GROUP BY priority_group
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_csat, ROUND(s.csat - g.max_csat, 2) AS csat_minus_max
    FROM unique_rows s
    JOIN priority_group_stats g ON g.priority_group = s.priority_group
    WHERE s.csat = g.max_csat
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2025-06-30', closed_at) AS days_since_closed_at,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM vs_group
),
latest AS (
    -- Keep the latest row per agent_id, priority_group
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        requester_id_minus_csat,
        priority_group,
        days_since_opened_at,
        max_csat,
        csat_minus_max,
        days_since_closed_at,
        priority_clean
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority_group ORDER BY closed_at DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', closed_at) AS closed_at_quarter,
        GREATEST(days_since_closed_at, requester_id_minus_csat) AS greatest_days_since_closed_at_requester_id_minus,
        COALESCE(requester_id_minus_csat, 0) AS requester_id_minus_csat_filled
    FROM latest
)
-- Final projection
SELECT
    id,
    closed_at,
    priority,
    csat,
    requester_id_minus_csat,
    priority_group,
    days_since_opened_at,
    max_csat,
    csat_minus_max,
    days_since_closed_at,
    priority_clean,
    closed_at_quarter,
    greatest_days_since_closed_at_requester_id_minus,
    CASE WHEN priority IN ('P4', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group_2
FROM scored
WHERE LENGTH(TRIM(priority_clean)) >= 8
ORDER BY greatest_days_since_closed_at_requester_id_minus
