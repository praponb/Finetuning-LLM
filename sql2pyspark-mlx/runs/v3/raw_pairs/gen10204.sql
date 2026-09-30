SELECT
    unit_price_bucket,
    quantity_bucket,
    revenue,
    stddev_quantity
FROM (
    SELECT
        CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 190 THEN 'mid' ELSE 'low' END AS unit_price_bucket,
        CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END AS quantity_bucket,
        SUM(quantity * unit_price) AS revenue,
        ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    GROUP BY CASE WHEN unit_price >= 420 THEN 'high' WHEN unit_price >= 190 THEN 'mid' ELSE 'low' END, CASE WHEN quantity >= 7 THEN 'high' WHEN quantity >= 3 THEN 'mid' ELSE 'low' END
) AS t
WHERE revenue < 42;
