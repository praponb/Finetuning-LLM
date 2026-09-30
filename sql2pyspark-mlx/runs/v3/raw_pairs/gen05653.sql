SELECT
    id,
    quantity,
    unit_price,
    CAST(unit_price AS INT) AS unit_price_int
FROM order_items
WHERE unit_price <= 110
ORDER BY id DESC;
