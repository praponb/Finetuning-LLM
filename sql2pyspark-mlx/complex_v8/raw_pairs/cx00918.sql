WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE (holder_id IS NOT NULL OR start_date >= DATE '2024-05-07')
),
status_stats AS (
    -- Avg premium per status
    SELECT status, AVG(premium) AS avg_premium
    FROM cleaned
    GROUP BY status
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM cleaned s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium > g.avg_premium
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT start_date, status, premium_minus_avg
    FROM outliers
) src
PIVOT (
    SUM(premium_minus_avg)
    FOR status IN ('CANCELLED', 'EXPIRED', 'ACTIVE')
)
