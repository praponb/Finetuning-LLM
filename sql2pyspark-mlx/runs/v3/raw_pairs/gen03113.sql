SELECT
    stores.id,
    stores.store_name,
    stores.opened_date,
    t.partner_count
FROM stores
LEFT OUTER JOIN (SELECT store_id, SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END) AS partner_count
    FROM orders
    GROUP BY store_id) t ON stores.id = t.store_id
ORDER BY id DESC, stores.opened_date ASC
