WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(CASE WHEN brand = 'Umbrella' THEN 1 END) AS umbrella_count
    FROM products
    GROUP BY supplier_id
)
SELECT s.id, s.rating, pbs.umbrella_count
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.umbrella_count > 7
ORDER BY rating, id
LIMIT 10;
