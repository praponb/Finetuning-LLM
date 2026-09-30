WITH supplier_totals AS (
    SELECT supplier_id, ROUND(STDDEV(stock_qty), 2) stddev_stock_qty
    FROM products
    WHERE category = 'Fashion'
    GROUP BY supplier_id
)
SELECT s.id, s.country, st.stddev_stock_qty
FROM suppliers s
INNER JOIN supplier_totals st ON s.id = st.supplier_id
