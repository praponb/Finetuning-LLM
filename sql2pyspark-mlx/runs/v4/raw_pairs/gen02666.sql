WITH supplier_totals AS (
    SELECT supplier_id, MIN(cost) AS min_cost
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'premium')
    GROUP BY supplier_id
)
SELECT s.id, s.rating, st.min_cost
FROM suppliers s
LEFT OUTER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.min_cost >= 650
