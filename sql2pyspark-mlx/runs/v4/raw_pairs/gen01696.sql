WITH supplier_totals AS (
    SELECT supplier_id, MAX(launch_date) AS last_launch_date, SUM(stock_qty) AS total_stock_qty
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.city,
    st.last_launch_date,
    st.total_stock_qty
FROM suppliers s
JOIN supplier_totals st ON s.id = st.supplier_id
