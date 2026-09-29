WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2026-01-01', opened_at) AS days_since_opened_at
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 2)
      AND HOUR(opened_at) BETWEEN 10 AND 14
),
totals AS (
    -- Group-level totals
    SELECT
        csat,
        MAX(days_since_opened_at) AS max_days_since_opened_at,
        COUNT(*) AS n_rows,
        SUM(csat) AS total_csat,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM filtered
    WHERE csat >= 2
    GROUP BY 1
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM totals
    WHERE total_csat > (SELECT AVG(total_csat) FROM totals)
),
annotated AS (
    -- Label and score rows
    SELECT
        csat,
        n_rows,
        total_csat,
        priority_list,
        n_rows - total_csat AS n_rows_minus_total_csat,
        FLOOR(total_csat / 5) * 5 AS total_csat_bucket,
        ROUND(csat / NULLIF(max_days_since_opened_at, 0), 3) AS csat_per_max_days_since_opened_at
    FROM with_activity
    WHERE csat <= 4
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM annotated
    WHERE total_csat < (SELECT MAX(total_csat) FROM annotated)
)
SELECT
    csat,
    n_rows_minus_total_csat,
    CASE WHEN total_csat >= 23 THEN 'HIGH' WHEN total_csat >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS total_csat_tier,
    ROUND(total_csat_bucket / NULLIF(total_csat, 0), 3) AS total_csat_bucket_per_total_csat
FROM screened
WHERE n_rows_minus_total_csat BETWEEN 9 AND 38
ORDER BY n_rows_minus_total_csat, total_csat_bucket_per_total_csat DESC, csat
LIMIT 10
