SELECT
    supplier_id,
    CASE WHEN brand IN ('Initech', 'Globex') THEN 'group_a' ELSE 'other' END AS brand_group,
    ROUND(AVG(cost), 2) AS avg_cost,
    AVG(stock_qty) AS avg_stock_qty
FROM products
WHERE price > 1160 OR category IN ('Grocery', 'Sports', 'Books')
GROUP BY supplier_id, brand_group
