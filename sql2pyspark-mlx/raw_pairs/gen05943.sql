SELECT
    id,
    quantity,
    order_id,
    COALESCE(quantity, 0) AS quantity_filled,
    CASE WHEN unit_price >= 350 THEN 'High' WHEN unit_price >= 60 THEN 'Medium' ELSE 'Low' END AS unit_price_band
FROM order_items
WHERE unit_price <= 290 AND quantity < 3
ORDER BY id DESC;
