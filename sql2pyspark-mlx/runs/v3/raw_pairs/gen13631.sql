SELECT
    brand,
    category,
    COUNT(DISTINCT price) unique_categorys,
    COUNT(price) price_count,
    SORT_ARRAY(COLLECT_SET(brand)) brand_list
FROM products
WHERE supplier_id = 23 OR supplier_id IN (26, 30, 33)
GROUP BY brand, category;
