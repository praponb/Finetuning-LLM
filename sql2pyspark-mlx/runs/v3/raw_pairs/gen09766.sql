SELECT channel, status, stddev_discount
FROM (
    SELECT channel, status, ROUND(STDDEV(discount), 2) AS stddev_discount
    FROM orders
    GROUP BY channel, status
) AS s
WHERE stddev_discount < 39;
