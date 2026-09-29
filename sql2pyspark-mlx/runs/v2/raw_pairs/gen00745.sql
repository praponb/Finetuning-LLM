SELECT
    brand,
    SUM(CASE WHEN category = 'Grocery' THEN stock_qty ELSE 0 END) grocery_stock_qty,
    SUM(CASE WHEN category = 'Home' THEN cost ELSE 0 END) home_cost,
    SORT_ARRAY(COLLECT_SET(brand)) brand_list
FROM products
WHERE supplier_id = 36 AND supplier_id IS NOT NULL
GROUP BY brand
ORDER BY home_cost, 2, brand
LIMIT 3
