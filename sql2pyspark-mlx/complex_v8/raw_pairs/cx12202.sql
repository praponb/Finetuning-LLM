WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.07, 2) AS premium_adj
    FROM policies
    WHERE premium > 1541.8
      AND end_date BETWEEN DATE '2025-11-20' AND DATE '2027-03-20'
),
combined_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        p.holder_name
    FROM base s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
product_stats AS (
    -- Avg premium per product
    SELECT product, AVG(premium) AS avg_premium
    FROM combined_details_holder
    GROUP BY product
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_premium,
        ROUND(s.premium - g.avg_premium, 2) AS premium_minus_avg
    FROM combined_details_holder s
    JOIN product_stats g ON g.product = s.product
    WHERE s.premium > g.avg_premium
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT end_date, status, product, avg_premium
    FROM above_average
) src
PIVOT (
    MAX(avg_premium)
    FOR product IN ('TRAVEL', 'AUTO')
)
ORDER BY end_date, status
