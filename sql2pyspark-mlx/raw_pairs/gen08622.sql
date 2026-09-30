WITH valid AS (
    SELECT
        id,
        product_name,
        price,
        launch_date,
        tags,
        ROUND(price, 2) AS price_rounded
    FROM products
    WHERE launch_date >= TO_DATE('2023-06-26')
      AND stock_qty >= 107
),
base AS (
    SELECT
        id,
        price,
        launch_date,
        tags,
        price_rounded,
        LOWER(product_name) AS product_name_lower,
        DATE_FORMAT(launch_date, 'MMM yyyy') AS launch_date_label
    FROM valid
    WHERE price < 1100
),
metrics AS (
    SELECT
        *,
        SUM(price_rounded) OVER w_grp AS running_sum_price_rounded,
        AVG(price_rounded) OVER w_grp AS running_avg_price_rounded,
        MIN(price_rounded) OVER w_grp AS running_min_price_rounded
    FROM base
    WINDOW w_grp AS (ORDER BY price_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    SELECT id
    FROM metrics
    INTERSECT
    SELECT id
    FROM products
    WHERE SIZE(tags) >= 1
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.id = k.id
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY running_avg_price_rounded NULLS LAST) AS running_avg_price_rounded_dense_rank,
        MIN(running_sum_price_rounded) OVER (PARTITION BY YEAR(launch_date)) AS launch_date_year_min_running_sum_price_rounded,
        DENSE_RANK() OVER w_running AS running_avg_price_rounded_dense_rank_2
    FROM reconciled
    WINDOW w_running AS (PARTITION BY DATE_TRUNC('YEAR', launch_date) ORDER BY running_avg_price_rounded NULLS LAST)
)
SELECT
    launch_date,
    tags,
    price_rounded,
    product_name_lower,
    launch_date_label,
    running_sum_price_rounded,
    running_avg_price_rounded,
    running_min_price_rounded,
    running_avg_price_rounded_dense_rank
FROM scored
ORDER BY price_rounded DESC;
