WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(product)) AS product_len
    FROM policies
    WHERE YEAR(end_date) = 2026
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        product_len,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        NTILE(4) OVER w_product_by_premium AS premium_quartile,
        LAG(start_date, 2) OVER w_product_by_premium AS prev2_start_date,
        ROW_NUMBER() OVER w_product_by_premium AS rn,
        LAG(premium) OVER (PARTITION BY product ORDER BY premium, id) AS prev_premium,
        ROUND(AVG(product_len) OVER (PARTITION BY product ORDER BY premium, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS product_len_avg_7
    FROM cleaned_policys
    WINDOW w_product_by_premium AS (PARTITION BY product ORDER BY premium, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        start_date,
        product_len,
        premium_quartile,
        rn,
        product_len_avg_7,
        COUNT(*) OVER w_end_date_month AS rows_in_end_date_month,
        FIRST_VALUE(end_date) OVER (ORDER BY last_known_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        LAST_VALUE(prev_premium) OVER (ORDER BY last_known_start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_premium,
        SIZE(COLLECT_SET(holder_id) OVER w_end_date_month) AS distinct_holder_id_in_end_date_month
    FROM with_windows
    WINDOW w_end_date_month AS (PARTITION BY DATE_TRUNC('MONTH', end_date))
),
top_rows AS (
    -- Top 5 per group
    SELECT
        id,
        holder_id,
        start_date,
        product_len,
        premium_quartile,
        product_len_avg_7,
        rows_in_end_date_month,
        first_end_date,
        last_prev_premium,
        distinct_holder_id_in_end_date_month
    FROM ranked
    WHERE rn <= 5
      AND start_date < DATE '2025-03-24'
)
SELECT
    product_len,
    holder_id,
    ROUND(SUM(distinct_holder_id_in_end_date_month) / NULLIF(SUM(product_len), 0), 3) AS distinct_holder_id_in_end_date_month_to_product_,
    ROUND(SUM(last_prev_premium), 2) AS total_last_prev_premium,
    DATEDIFF(MAX(first_end_date), MIN(first_end_date)) AS first_end_date_span_days
FROM top_rows
GROUP BY GROUPING SETS ((product_len, holder_id), (product_len))
ORDER BY product_len NULLS LAST, holder_id NULLS LAST
