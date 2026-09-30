SELECT
    brand,
    MONTH(launch_date) AS launch_month,
    supplier_id,
    MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range,
    COUNT(*) AS total_count,
    COUNT(CASE WHEN brand = 'Globex' THEN 1 END) AS globex_count
FROM products
WHERE product_name LIKE '%Laptop%'
GROUP BY brand, launch_month, supplier_id
HAVING COUNT(*) < 2
ORDER BY supplier_id;
