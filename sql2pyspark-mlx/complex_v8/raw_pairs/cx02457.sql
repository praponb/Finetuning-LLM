WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        CONCAT_WS(': ', product, status) AS product_status_label
    FROM policies
    WHERE (status = 'LAPSED' OR product <> 'HOME')
),
unique_rows AS (
    -- Keep the latest row per product
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        premium_filled,
        product_status_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY start_date DESC, id DESC) AS rn
        FROM recent_policys
    ) ranked
    WHERE rn = 1
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(premium_filled) OVER (PARTITION BY status, MONTH(start_date)) AS status_start_date_month_sum_premium_filled,
        premium_filled - LAG(premium_filled) OVER (ORDER BY premium ASC, id) AS premium_filled_change,
        MAX(premium_filled) OVER (PARTITION BY status, MONTH(start_date)) AS status_start_date_month_max_premium_filled,
        ROUND(AVG(premium_filled) OVER (PARTITION BY status, MONTH(start_date)), 2) AS status_start_date_month_avg_premium_filled
    FROM unique_rows
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT holder_id, product, premium
    FROM ranked
) src
PIVOT (
    COUNT(premium) AS count_v, SUM(premium) AS sum_v
    FOR product IN ('AUTO', 'HOME', 'HEALTH', 'TRAVEL')
)
