WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug
    FROM policies
    WHERE product NOT IN ('LIFE', 'HOME')
      AND start_date < DATE '2025-02-13'
),
status_stats AS (
    -- Median premium per status
    SELECT status, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM valid
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM valid s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium >= g.median_premium
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date, status, median_premium
    FROM above_average
) src
PIVOT (
    SUM(median_premium) AS total, COUNT(median_premium) AS n
    FOR status IN ('ACTIVE', 'CANCELLED')
)
