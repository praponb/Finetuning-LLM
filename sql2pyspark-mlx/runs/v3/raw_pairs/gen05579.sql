SELECT
    stores.sqft,
    stores.city,
    inventory.last_restock,
    inventory.product_id
FROM stores
LEFT JOIN inventory ON inventory.store_id = stores.id
WHERE stores.store_name NOT LIKE 'Si%';
