SELECT
    id,
    product_name,
    brand,
    cost,
    launch_date,
    supplier_id
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC, id) AS dedup_rank
    FROM products
) x
WHERE dedup_rank = 1
