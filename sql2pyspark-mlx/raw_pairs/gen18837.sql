SELECT category, stddev_price
FROM (
    SELECT category, ROUND(STDDEV(price), 2) stddev_price
    FROM products
    WHERE SIZE(tags) >= 2
    GROUP BY category
) s
WHERE stddev_price < 26
ORDER BY category;
