SELECT
    brand,
    MAX(cost) - MIN(cost) cost_range,
    ROUND(STDDEV(price), 2) stddev_price,
    AVG(cost) avg_cost
FROM products
WHERE supplier_id IN (1, 19, 30) OR ARRAY_CONTAINS(tags, 'bestseller')
GROUP BY brand;
