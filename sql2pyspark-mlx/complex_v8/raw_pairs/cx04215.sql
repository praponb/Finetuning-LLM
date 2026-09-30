WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM policies
    WHERE (holder_id IS NOT NULL OR premium <= 4678.8)
),
status_stats AS (
    -- Avg premium per status
    SELECT status, AVG(premium) AS avg_premium
    FROM valid
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM valid s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium > g.avg_premium
)
SELECT *
FROM (
    SELECT holder_id, end_date, status, avg_premium
    FROM outliers
) src
PIVOT (
    COUNT(avg_premium)
    FOR status IN ('EXPIRED', 'ACTIVE')
)
ORDER BY holder_id, end_date
