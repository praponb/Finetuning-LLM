SELECT
    p.id,
    p.launch_date,
    p.price,
    p.product_name,
    s.country,
    s.supplier_name,
    s.rating,
    DATEDIFF(CURRENT_DATE(), p.launch_date) AS days_since_launch_date
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE p.brand IN ('Umbrella', 'Stark');
