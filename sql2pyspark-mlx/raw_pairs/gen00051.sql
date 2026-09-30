SELECT
    brand,
    ROUND(STDDEV(cost), 2) stddev_cost,
    COUNT(*) total_count,
    MIN(cost) min_cost
FROM products
WHERE price < 640
GROUP BY 1
ORDER BY stddev_cost DESC, min_cost;
