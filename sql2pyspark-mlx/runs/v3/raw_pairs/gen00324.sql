WITH supplier_totals AS (
    SELECT supplier_id, COUNT(DISTINCT stock_qty) AS unique_suppliers, ROUND(STDDEV(price), 2) AS stddev_price
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.supplier_name,
    st.unique_suppliers,
    st.stddev_price
FROM suppliers s
JOIN supplier_totals st ON s.id = st.supplier_id
WHERE st.unique_suppliers >= 10
ORDER BY id DESC, supplier_name DESC
LIMIT 10;
