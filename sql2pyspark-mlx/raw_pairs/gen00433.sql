WITH base AS (
    SELECT
        *,
        INITCAP(TRIM(store_name)) AS store_name_title,
        CAST(sqft AS DOUBLE) AS sqft_num
    FROM stores
    WHERE opened_date > TO_DATE('2023-04-06')
      AND region IS NOT NULL
)
SELECT
    id,
    city,
    region,
    opened_date,
    store_name_title,
    sqft_num,
    LEAD(sqft_num) OVER (PARTITION BY city ORDER BY sqft, id) AS next_sqft_num,
    LAG(sqft) OVER (PARTITION BY city ORDER BY sqft, id) AS prev_sqft,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS seq_no,
    MIN(sqft) OVER (PARTITION BY DATE_TRUNC('month', opened_date), city) AS opened_date_period_min_sqft
FROM base
