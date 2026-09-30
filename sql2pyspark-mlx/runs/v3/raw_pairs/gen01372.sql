SELECT
    stores.store_name,
    stores.sqft,
    stores.opened_date,
    inventory.quantity
FROM stores
LEFT JOIN inventory ON stores.id = inventory.store_id
ORDER BY quantity DESC, opened_date;
