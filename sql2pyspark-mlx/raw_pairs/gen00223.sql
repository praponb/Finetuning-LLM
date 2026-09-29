WITH supplier_totals AS (
    SELECT supplier_id, SUM(CASE WHEN category = 'Grocery' THEN price ELSE 0 END) AS grocery_price
    FROM products
    WHERE UPPER(TRIM(product_name)) = 'NOISE-CANCEL HEADPHONES'
    GROUP BY supplier_id
)
SELECT suppliers.id, suppliers.city, supplier_totals.grocery_price
FROM suppliers
INNER JOIN supplier_totals ON suppliers.id = supplier_totals.supplier_id
WHERE supplier_totals.grocery_price >= 46
ORDER BY id DESC
