WITH products_by_supplier AS (
    SELECT supplier_id, COUNT(DISTINCT price) AS unique_categorys
    FROM products
    WHERE stock_qty < 209
    GROUP BY supplier_id
)
SELECT s.country, s.rating, pbs.unique_categorys
FROM suppliers s
LEFT JOIN products_by_supplier pbs ON s.id = pbs.supplier_id
WHERE pbs.unique_categorys >= 8
ORDER BY rating DESC, country
