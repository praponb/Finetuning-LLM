WITH products_by_supplier AS (
    SELECT supplier_id, MAX(launch_date) AS last_launch_date, MAX(price) AS max_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.country,
    s.supplier_name,
    pbs.last_launch_date,
    pbs.max_price
FROM suppliers s
INNER JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.max_price >= 410;
