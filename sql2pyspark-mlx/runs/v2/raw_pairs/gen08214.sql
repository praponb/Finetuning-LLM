SELECT
    category,
    tags,
    stock_qty,
    DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_date_label
FROM products
WHERE (cost >= 350 AND launch_date > '2024-08-16') OR category IN ('Books', 'Grocery')
