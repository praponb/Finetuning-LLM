SELECT
    inventory.store_id,
    stores.city,
    stores.opened_date,
    products.cost,
    products.launch_date,
    DATEDIFF(CURRENT_DATE(), stores.opened_date) days_since_opened_date
FROM inventory
INNER JOIN stores ON inventory.store_id = stores.id
LEFT OUTER JOIN products ON products.id = inventory.product_id
WHERE stores.sqft BETWEEN 11064 AND 13458
