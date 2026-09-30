SELECT
    id,
    cost,
    DAYOFWEEK(launch_date) AS launch_date_dayofweek,
    price * stock_qty AS price_times_stock_qty
FROM products
ORDER BY id DESC;
