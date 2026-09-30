SELECT
    id,
    product_id,
    unit_price,
    CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 4 THEN 'Medium' ELSE 'Low' END AS quantity_band
FROM order_items;
