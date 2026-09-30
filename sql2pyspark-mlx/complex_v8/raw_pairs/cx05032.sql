WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        DATEDIFF(DATE '2025-12-31', start_date) AS days_since_start_date
    FROM policies
),
summary AS (
    -- Summarise per group
    SELECT
        product,
        COUNT(*) AS n_rows,
        MIN(days_since_start_date) AS min_days_since_start_date,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list,
        ROUND(SUM(days_since_start_date) / NULLIF(SUM(premium), 0), 3) AS days_since_start_date_to_premium_ratio,
        ROUND(STDDEV(premium), 2) AS stddev_premium
    FROM recent
    WHERE days_since_start_date BETWEEN 35 AND 455
    GROUP BY 1
),
scored AS (
    -- Label and score rows
    SELECT
        product,
        n_rows,
        min_days_since_start_date,
        product_list,
        days_since_start_date_to_premium_ratio,
        GREATEST(n_rows, stddev_premium) AS greatest_n_rows_stddev_premium
    FROM summary
    WHERE stddev_premium > 2000.0
),
annotated AS (
    -- Add calculated columns
    SELECT
        product,
        n_rows,
        min_days_since_start_date,
        product_list,
        days_since_start_date_to_premium_ratio,
        greatest_n_rows_stddev_premium,
        ARRAY_JOIN(ARRAY_SORT(product_list), ',') AS product_list_text,
        ABS(n_rows - 6) AS n_rows_distance
    FROM scored
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM annotated
    WHERE n_rows < (SELECT MAX(n_rows) FROM annotated)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM qualified
    WHERE min_days_since_start_date > (SELECT AVG(min_days_since_start_date) FROM qualified)
)
-- Final projection
SELECT
    product,
    min_days_since_start_date,
    days_since_start_date_to_premium_ratio,
    product_list_text,
    n_rows_distance,
    SIZE(product_list) AS product_list_count
FROM screened
ORDER BY n_rows_distance DESC NULLS FIRST, product DESC
