WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ROUND(premium * 1.07, 2) AS premium_adj,
        COALESCE(premium, 0.0) AS premium_filled,
        ABS(premium - 2289.0) AS premium_distance
    FROM policies
    WHERE holder_id IS NOT NULL
      AND end_date >= DATE '2026-01-23'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_ym,
        product,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        COUNT(*) AS cnt,
        DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
        MIN(premium_adj) AS min_premium_adj,
        MIN(premium_filled) AS min_premium_filled,
        MIN(premium) AS min_premium
    FROM cleaned
    GROUP BY 1, 2, 3
    HAVING SUM(premium_distance) > 750.0
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.holder_id,
        s.start_date_ym,
        s.product,
        s.distinct_holder_ids,
        s.start_date_span_days,
        s.min_premium,
        p.holder_name
    FROM stats s
    LEFT JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    holder_id,
    start_date_ym,
    product,
    distinct_holder_ids,
    start_date_span_days,
    min_premium,
    holder_name,
    SUBSTRING(TRIM(product), 1, 3) AS product_prefix
FROM with_details_holder
