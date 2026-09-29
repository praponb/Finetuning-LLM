SELECT
    stock_qty,
    category,
    brand,
    MONTH(launch_date) launch_date_month
FROM products
WHERE supplier_id = 11
