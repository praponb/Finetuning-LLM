WITH supplier_totals AS (
    SELECT supplier_id, ROUND(AVG(cost), 2) AS avg_cost
    FROM products
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.city,
    st.avg_cost
FROM suppliers s
LEFT JOIN supplier_totals st ON s.id = st.supplier_id;
