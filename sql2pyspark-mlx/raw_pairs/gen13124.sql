WITH enriched AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY category ORDER BY price, id) AS price_bucket,
        CORR(cost, price) OVER (PARTITION BY MONTH(launch_date)) AS corr_cost_price,
        SUM(stock_qty) OVER (PARTITION BY brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty
    FROM products
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY corr_cost_price DESC) AS corr_cost_price_dense_rank,
        NTILE(3) OVER (PARTITION BY category ORDER BY running_stock_qty, id) AS running_stock_qty_bucket,
        AVG(stock_qty) OVER (PARTITION BY category) AS category_avg_stock_qty
    FROM enriched
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY brand ORDER BY cost DESC) AS rn
    FROM metrics
    QUALIFY rn = 1
),
enriched_products AS (
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        stock_qty,
        tags,
        price_bucket,
        running_stock_qty,
        category_avg_stock_qty,
        COUNT(corr_cost_price) OVER (PARTITION BY category ORDER BY category_avg_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_corr_cost_price,
        MIN(corr_cost_price) OVER (PARTITION BY category ORDER BY category_avg_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_corr_cost_price,
        MAX(corr_cost_price) OVER (PARTITION BY category ORDER BY category_avg_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_corr_cost_price,
        AVG(corr_cost_price) OVER (PARTITION BY category ORDER BY category_avg_stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_corr_cost_price,
        SUM(cost) OVER (PARTITION BY brand ORDER BY corr_cost_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM ranked
    WHERE running_stock_qty = 614
),
scored AS (
    SELECT
        *,
        LEAD(last7_max_corr_cost_price) OVER (PARTITION BY brand ORDER BY running_cost, id) AS next_last7_max_corr_cost_price,
        LAG(last7_max_corr_cost_price) OVER (PARTITION BY brand ORDER BY running_cost, id) AS prev_last7_max_corr_cost_price,
        LAG(price_bucket) OVER (PARTITION BY brand ORDER BY running_cost, id) AS prev_price_bucket,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY running_cost, id) AS seq_no
    FROM enriched_products
)
SELECT
    id,
    product_name,
    brand,
    price,
    stock_qty,
    tags,
    price_bucket,
    category_avg_stock_qty,
    last7_count_corr_cost_price,
    CASE WHEN product_name RLIKE '^(Office Chair|Yoga Mat)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS product_name_zone
FROM scored
ORDER BY price DESC;
