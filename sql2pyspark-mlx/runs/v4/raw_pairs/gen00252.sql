WITH supplier_totals AS (
    SELECT supplier_id, ROUND((100 * SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_acme, SUM(stock_qty) AS total_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.rating,
    s.country,
    st.pct_acme,
    st.total_stock_qty
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id;
