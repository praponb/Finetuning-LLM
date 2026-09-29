SELECT
    supplier_id,
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    CASE WHEN price >= 1160 THEN 'LARGE' ELSE 'SMALL' END AS price_tier,
    ROUND(SUM(stock_qty) / NULLIF(COUNT(CASE WHEN brand = 'Stark' THEN 1 END), 0), 2) AS stock_qty_per_stark,
    CORR(stock_qty, price) AS corr_stock_qty_price,
    SUM(cost) FILTER (WHERE category = 'Fashion') AS fashion_total_cost,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SUM(cost) FILTER (WHERE category = 'Electronics') AS electronics_total_cost,
    COUNT(CASE WHEN category = 'Sports' THEN 1 END) AS sports_count
FROM products
GROUP BY supplier_id, quarter_start, price_tier
HAVING CORR(stock_qty, price) > 0
ORDER BY supplier_id NULLS FIRST;
