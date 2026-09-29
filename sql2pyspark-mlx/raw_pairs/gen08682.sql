SELECT
    id,
    category,
    supplier_id,
    price
FROM products
WHERE stock_qty BETWEEN 243 AND 267 AND MONTH(launch_date) IN (4, 5, 6)
ORDER BY 4
