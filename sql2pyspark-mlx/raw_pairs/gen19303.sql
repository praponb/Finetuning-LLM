SELECT
    stores.id,
    stores.region,
    stores.city,
    stores.sqft
FROM stores
WHERE stores.id IN (SELECT store_id FROM orders);
