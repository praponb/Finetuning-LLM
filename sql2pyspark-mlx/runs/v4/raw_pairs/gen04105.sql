WITH scored AS (
    SELECT
        *,
        LEAD(stock_qty, 2) OVER w_hist AS next_stock_qty,
        RANK() OVER (PARTITION BY category, brand ORDER BY price DESC NULLS LAST) AS price_rank,
        SUM(price) OVER w_running AS cumulative_price,
        DENSE_RANK() OVER w_part AS cost_dense_rank
    FROM products
    WINDOW w_hist AS (ORDER BY launch_date, id), w_running AS (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (ORDER BY cost NULLS LAST)
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.launch_date,
    s.tags,
    s.supplier_id,
    s.next_stock_qty,
    s.cumulative_price,
    CASE WHEN product_name RLIKE '^(Office Chair|Running Shoes)$' THEN 'GROUP_B' WHEN product_name RLIKE '^(Laptop Pro 14|wireless mouse)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS product_name_zone,
    (SELECT MAX(x.price) FROM products x WHERE x.supplier_id = s.supplier_id) AS max_product_price
FROM scored s
ORDER BY cumulative_price NULLS LAST, brand
