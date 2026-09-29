SELECT *
FROM (
    SELECT
        id,
        store_id,
        order_date,
        RANK() OVER (PARTITION BY store_id ORDER BY order_date) AS rnk
    FROM orders
) AS r
WHERE rnk <= 3
ORDER BY order_date;
