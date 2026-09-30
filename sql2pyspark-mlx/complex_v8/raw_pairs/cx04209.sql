WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        ABS(premium - 2096.6) AS premium_distance
    FROM policies
    WHERE holder_id <= 3
      AND status IN ('LAPSED', 'EXPIRED', 'ACTIVE')
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        COUNT(*) AS cnt,
        COUNT(DISTINCT product_or_unknown) AS distinct_product_or_unknowns
    FROM base
    WHERE product_or_unknown = 'TRAVEL'
    GROUP BY DATE_TRUNC('MONTH', start_date)
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM totals
    WHERE cnt < (SELECT MAX(cnt) FROM totals)
),
screened_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM screened
    WHERE distinct_product_or_unknowns > (SELECT AVG(distinct_product_or_unknowns) FROM screened)
)
-- Final projection
SELECT start_date_month, distinct_product_or_unknowns
FROM screened_2
ORDER BY distinct_product_or_unknowns DESC, start_date_month
