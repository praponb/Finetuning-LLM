WITH supplier_totals AS (
    SELECT supplier_id, MAX(stock_qty) AS max_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.country,
    st.max_stock_qty
FROM suppliers s
JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.max_stock_qty >= 390
ORDER BY id DESC, s.rating
LIMIT 10;
