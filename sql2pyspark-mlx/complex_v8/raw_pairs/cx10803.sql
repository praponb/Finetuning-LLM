WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        tags,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year,
        DATEDIFF(DATE '2026-01-01', closed_at) AS days_since_closed_at
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-24 01:00:00'
),
stats AS (
    -- Summarise per group
    SELECT
        priority,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        MIN(days_since_closed_at) AS min_days_since_closed_at,
        ROUND(SUM(requester_id) / NULLIF(SUM(days_since_closed_at), 0), 3) AS requester_id_to_days_since_closed_at_ratio,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        COUNT(*) AS cnt
    FROM recent
    GROUP BY priority, DATE_FORMAT(opened_at, 'yyyy-MM')
),
scored AS (
    -- Derive helper columns
    SELECT *, COALESCE(cnt, 0) AS cnt_filled
    FROM stats
    WHERE opened_at_ym IS NOT NULL
),
qualified AS (
    -- Compare against the overall avg
    SELECT *
    FROM scored
    WHERE min_days_since_closed_at > (SELECT AVG(min_days_since_closed_at) FROM scored)
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT cnt, cnt_filled, priority, requester_id_to_days_since_closed_at_ratio
    FROM qualified
) src
PIVOT (
    AVG(requester_id_to_days_since_closed_at_ratio)
    FOR priority IN ('P3', 'P4', 'P2', 'P1')
)
ORDER BY cnt, cnt_filled
