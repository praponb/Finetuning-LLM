WITH products_by_supplier AS (
    SELECT supplier_id, ROUND(STDDEV(price), 2) stddev_price
    FROM products
    GROUP BY supplier_id
)
SELECT s.city, s.rating, pbs.stddev_price
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.stddev_price >= 45;
