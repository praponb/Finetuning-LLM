SELECT brand, min_stock_qty, max_cost
FROM (
    SELECT brand, MIN(stock_qty) AS min_stock_qty, MAX(cost) AS max_cost
    FROM products
    WHERE supplier_id IS NOT NULL
    GROUP BY brand
) AS agg
WHERE min_stock_qty > 140
ORDER BY 2 DESC;
