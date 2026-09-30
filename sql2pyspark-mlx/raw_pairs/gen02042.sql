SELECT
    id,
    supplier_id,
    price,
    DENSE_RANK() OVER (PARTITION BY supplier_id ORDER BY price DESC) AS price_dense_rank
FROM products
WHERE cost >= 560;
