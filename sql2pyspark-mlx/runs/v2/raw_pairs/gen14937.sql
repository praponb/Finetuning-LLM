WITH products_by_supplier AS (
    SELECT supplier_id, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM products
    WHERE price < 1050
    GROUP BY supplier_id
)
SELECT s.country, s.city, pbs.stddev_cost
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
ORDER BY stddev_cost;
