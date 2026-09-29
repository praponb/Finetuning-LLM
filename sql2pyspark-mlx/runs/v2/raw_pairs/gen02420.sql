SELECT
    id,
    brand,
    supplier_id,
    launch_date,
    cost,
    SUM(cost) OVER (PARTITION BY brand, supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
FROM products
WHERE cost BETWEEN 270 AND 690
