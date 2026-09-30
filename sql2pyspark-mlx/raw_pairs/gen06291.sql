SELECT supplier_id, stddev_cost
FROM (
    SELECT supplier_id, ROUND(STDDEV(cost), 2) stddev_cost
    FROM products
    GROUP BY supplier_id
) agg
WHERE stddev_cost > 62
ORDER BY stddev_cost;
