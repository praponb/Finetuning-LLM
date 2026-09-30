WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(*) AS num_products, MIN(launch_date) AS min_launch_date
    FROM products
    GROUP BY supplier_id
)
SELECT s.supplier_name, pbs.num_products, pbs.min_launch_date
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY s.supplier_name;
