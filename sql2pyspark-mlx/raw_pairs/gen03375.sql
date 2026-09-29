SELECT supplier_id, MAX(price) - MIN(price) AS price_range, SORT_ARRAY(COLLECT_SET(brand)) AS brand_list
FROM products
GROUP BY supplier_id;
