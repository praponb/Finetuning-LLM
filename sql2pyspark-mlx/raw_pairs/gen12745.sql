SELECT
    id,
    quantity,
    product_id,
    store_id,
    CASE WHEN quantity >= 245 THEN 'High' WHEN quantity >= 90 THEN 'Medium' ELSE 'Low' END quantity_tier,
    LAST_DAY(last_restock) last_restock_month_end
FROM inventory
WHERE last_restock <= '2025-03-01';
