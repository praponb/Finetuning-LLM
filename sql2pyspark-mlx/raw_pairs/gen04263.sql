SELECT
    id,
    price,
    cost,
    launch_date,
    tags,
    supplier_id
FROM (
    SELECT *, RANK() OVER (PARTITION BY category, brand ORDER BY cost DESC) AS dedup_rank
    FROM products
) x
WHERE dedup_rank = 1;
