WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len,
        CONCAT_WS(': ', status, product) AS status_product_label,
        CONCAT_WS('-', status, product) AS status_product_label_2
    FROM policies
    WHERE holder_id >= 3
),
status_stats AS (
    -- Median premium per status
    SELECT status, PERCENTILE_APPROX(premium, 0.5) AS median_premium
    FROM recent
    GROUP BY status
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium,
        ROUND(s.premium - g.median_premium, 2) AS premium_minus_median
    FROM recent s
    JOIN status_stats g ON g.status = s.status
    WHERE s.premium >= g.median_premium
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        holder_id,
        ROUND(SUM(premium_minus_median) / NULLIF(SUM(product_len), 0), 3) AS premium_minus_median_to_product_len_ratio,
        MAX(end_date) AS last_end_date,
        MIN(premium_minus_median) AS min_premium_minus_median,
        COUNT(*) AS cnt,
        ROUND(AVG(premium), 2) AS avg_premium,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM above_average
    GROUP BY status, holder_id
    HAVING COUNT(*) >= 2
)
SELECT *
FROM (
    SELECT cnt, status, avg_premium
    FROM rollup_base
) src
PIVOT (
    SUM(avg_premium)
    FOR status IN ('EXPIRED', 'CANCELLED')
)
ORDER BY cnt
