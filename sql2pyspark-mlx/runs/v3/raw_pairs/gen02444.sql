WITH products_by_supplier AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Sports' THEN cost ELSE 0 END) AS sports_cost
    FROM products
    WHERE product_name LIKE '%Sma%'
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    s.country,
    pbs.sports_cost
FROM suppliers s
JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.sports_cost > 74
