SELECT
    supplier_id,
    CASE WHEN brand IN ('Acme', 'Stark') THEN 'priority' ELSE 'other' END brand_group,
    category,
    MAX(price) max_price,
    SORT_ARRAY(COLLECT_SET(category)) category_list,
    MIN(cost) min_cost
FROM products
GROUP BY 1, 2, 3
