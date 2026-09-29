SELECT
    id,
    supplier_id,
    launch_date,
    price,
    SUM(price) OVER (PARTITION BY supplier_id ORDER BY launch_date, id) AS running_price
FROM products;
