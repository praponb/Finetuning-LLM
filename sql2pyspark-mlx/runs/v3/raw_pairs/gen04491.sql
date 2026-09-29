WITH supplier_totals AS (
    SELECT supplier_id, SUM(cost) total_cost
    FROM products
    WHERE stock_qty BETWEEN 115 AND 215
    GROUP BY supplier_id
)
SELECT s.rating, st.total_cost
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
ORDER BY st.total_cost DESC, rating
