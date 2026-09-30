SELECT price, CAST(cost AS INT) AS cost_int
FROM products
WHERE ARRAY_CONTAINS(tags, 'sale') AND cost <= 550
ORDER BY cost_int DESC, 1 DESC;
