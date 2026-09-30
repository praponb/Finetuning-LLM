WITH base AS (
    -- Scope the tickets rows
    SELECT *
    FROM tickets
    WHERE agent_id <= 4
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        DATE_TRUNC('MONTH', closed_at) AS closed_at_month,
        csat,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list,
        ROUND(AVG(csat), 2) AS avg_csat
    FROM base
    GROUP BY 1, 2, 3
    HAVING SUM(csat) > 3
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE avg_csat > (SELECT AVG(avg_csat) FROM grouped)
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE avg_csat > (SELECT AVG(avg_csat) FROM screened)
)
SELECT
    opened_at_month,
    closed_at_month,
    csat,
    channel_list,
    avg_csat
FROM screened_2
WHERE avg_csat >= 2.6
ORDER BY avg_csat DESC, csat DESC NULLS FIRST, opened_at_month, closed_at_month
LIMIT 10
