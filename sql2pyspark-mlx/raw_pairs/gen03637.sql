SELECT
    ss.units,
    p.category,
    p.product_name,
    DATE_SUB(p.launch_date, 5) AS launch_date_minus_5d
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
