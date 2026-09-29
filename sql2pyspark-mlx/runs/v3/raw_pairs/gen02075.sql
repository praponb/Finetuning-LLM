SELECT product_id, stddev_units
FROM (
    SELECT product_id, ROUND(STDDEV(units), 2) AS stddev_units
    FROM store_sales
    WHERE units <= 16
    GROUP BY product_id
) AS sub
WHERE stddev_units < 59
