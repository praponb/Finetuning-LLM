WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled,
        TRUNC(start_date, 'MM') AS start_date_month_start
    FROM policies
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-07-07'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER w_holder_id_by_premium AS seq_no,
        FIRST_VALUE(start_date_month_start) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date_month_start,
        LAST_VALUE(premium_filled) IGNORE NULLS OVER (ORDER BY start_date_month_start, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium_filled,
        ROUND(AVG(premium) OVER (ORDER BY start_date_month_start, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_6_prior,
        RANK() OVER w_all_by_start_date_month_star_3 AS start_date_month_start_rank,
        FIRST_VALUE(premium) OVER (ORDER BY start_date_month_start, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY MONTH(start_date_month_start))) AS distinct_status_in_start_date_month_start_month,
        DENSE_RANK() OVER (PARTITION BY product ORDER BY start_date DESC) AS start_date_dense_rank,
        DENSE_RANK() OVER (ORDER BY start_date_month_start) AS start_date_month_start_dense_rank,
        COUNT(*) OVER (PARTITION BY MONTH(start_date_month_start)) AS rows_in_start_date_month_start_month,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        MIN(premium_filled) OVER (PARTITION BY MONTH(start_date_month_start)) AS start_date_month_start_month_min_premium_filled,
        SIZE(COLLECT_SET(product) OVER w_start_date_month_start_month) AS distinct_product_in_start_date_month_start_month,
        NTH_VALUE(premium_filled, 2) OVER (ORDER BY start_date_month_start, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_filled
    FROM scoped
    WINDOW w_holder_id_by_premium AS (PARTITION BY holder_id ORDER BY premium DESC, id), w_all_by_start_date_month_star_3 AS (ORDER BY start_date_month_start), w_start_date_month_start_month AS (PARTITION BY MONTH(start_date_month_start))
),
annotated AS (
    -- Label and score rows
    SELECT *, COALESCE(premium_filled, 0.0) AS premium_filled_filled
    FROM ranked
    WHERE start_date_month_start_month_min_premium_filled > 17630.0
),
per_group AS (
    -- Summarise per group
    SELECT
        seq_no,
        DATE_TRUNC('MONTH', start_date_month_start) AS start_date_month_start_month,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        ROUND(SUM(start_date_dense_rank) / NULLIF(SUM(distinct_product_in_start_date_month_start_month), 0), 3) AS start_date_dense_rank_to_distinct_product_in_sta,
        PERCENTILE_APPROX(start_date_month_start_rank, 0.5) AS median_start_date_month_start_rank,
        MIN(second_premium_filled) AS min_second_premium_filled,
        MAX(start_date) AS last_start_date,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT product) AS distinct_products
    FROM annotated
    WHERE start_date_month_start_month_min_premium_filled BETWEEN 7670.0 AND 32570.0
    GROUP BY seq_no, DATE_TRUNC('MONTH', start_date_month_start), DATE_TRUNC('MONTH', start_date)
)
SELECT
    seq_no,
    start_date_month_start_month,
    start_date_dense_rank_to_distinct_product_in_sta,
    median_start_date_month_start_rank,
    min_second_premium_filled,
    last_start_date,
    n_rows,
    distinct_products,
    DATEDIFF(DATE '2025-12-31', last_start_date) AS days_since_last_start_date,
    CASE WHEN distinct_products >= 8 THEN 'A' WHEN distinct_products >= 5 THEN 'B' ELSE 'C' END AS distinct_products_tier
FROM per_group
ORDER BY seq_no DESC, last_start_date DESC
