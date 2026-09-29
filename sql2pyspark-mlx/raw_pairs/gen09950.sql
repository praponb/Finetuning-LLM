WITH scored AS (
    SELECT
        id,
        product_name,
        cost,
        tags,
        supplier_id,
        AVG(price) OVER w_grp AS price_avg_6,
        NTILE(3) OVER (PARTITION BY supplier_id ORDER BY cost DESC, id) AS cost_bucket,
        LEAD(price, 1) OVER (PARTITION BY MONTH(launch_date) ORDER BY stock_qty, id) AS next_price
    FROM products
    WHERE category <> 'Home'
    WINDOW w_grp AS (PARTITION BY supplier_id, category ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    product_name,
    cost,
    supplier_id,
    price_avg_6
FROM scored
ORDER BY cost DESC NULLS LAST
