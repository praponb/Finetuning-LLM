WITH supplier_totals AS (
    SELECT supplier_id, MIN(price) AS min_price
    FROM products
    WHERE category = 'Electronics'
    GROUP BY supplier_id
)
SELECT s.supplier_name, s.rating, st.min_price
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.min_price > 950
ORDER BY st.min_price ASC, 1 ASC;
