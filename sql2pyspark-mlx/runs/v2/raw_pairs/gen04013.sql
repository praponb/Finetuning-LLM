WITH supplier_summary AS (
    SELECT supplier_id, MAX(launch_date) AS last_launch_date
    FROM products
    WHERE cost IS NOT NULL
    GROUP BY supplier_id
)
SELECT suppliers.id, suppliers.country, supplier_summary.last_launch_date
FROM suppliers
LEFT JOIN supplier_summary ON suppliers.id = supplier_summary.supplier_id;
