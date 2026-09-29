WITH not_flagged AS (
    SELECT s.*
    FROM products s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock < s.launch_date
        )
),
latest AS (
    SELECT *
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) = 1
),
latest_products AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY launch_date DESC NULLS LAST, id) AS row_num
        FROM latest
    ) t
    WHERE row_num = 1
),
enriched AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        tags,
        supplier_id,
        MAX(stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_stock_qty,
        MIN(stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_stock_qty,
        SUM(stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_stock_qty,
        LEAD(stock_qty, 1) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS next_stock_qty
    FROM latest_products
)
SELECT
    CASE WHEN running_sum_stock_qty >= 1850 THEN 'LARGE' ELSE 'SMALL' END AS running_sum_stock_qty_tier,
    SIZE(COLLECT_SET(category)) AS num_categories,
    SUM(CASE WHEN brand = 'Umbrella' THEN running_sum_stock_qty ELSE 0 END) AS umbrella_running_sum_stock_qty,
    AVG(running_sum_stock_qty) FILTER (WHERE brand = 'Acme') AS acme_avg_running_sum_stock_qty,
    SUM(CASE WHEN brand = 'Stark' THEN running_max_stock_qty ELSE 0 END) AS stark_running_max_stock_qty,
    COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count,
    COUNT(CASE WHEN brand = 'Initech' THEN 1 END) AS initech_count
FROM enriched
GROUP BY CASE WHEN running_sum_stock_qty >= 1850 THEN 'LARGE' ELSE 'SMALL' END
HAVING stark_running_max_stock_qty < 1000000
