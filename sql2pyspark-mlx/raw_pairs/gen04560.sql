SELECT
    CASE WHEN category IN ('Fashion', 'Books') THEN 'core' ELSE 'other' END AS category_group,
    supplier_id,
    MAX(launch_date) AS latest_launch_date,
    COUNT(*) AS total_count
FROM products
WHERE product_name LIKE 'Noi%' AND ARRAY_CONTAINS(tags, 'bestseller')
GROUP BY CASE WHEN category IN ('Fashion', 'Books') THEN 'core' ELSE 'other' END, supplier_id
ORDER BY category_group DESC, supplier_id
LIMIT 3
