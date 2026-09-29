SELECT
    id,
    category,
    stock_qty,
    supplier_id,
    DATEDIFF(CURRENT_DATE(), launch_date) AS days_since_launch_date,
    SORT_ARRAY(tags) AS sorted_tags
FROM products
WHERE NOT ((price <= 590 AND category IN ('Fashion', 'Books')) OR supplier_id = 31);
