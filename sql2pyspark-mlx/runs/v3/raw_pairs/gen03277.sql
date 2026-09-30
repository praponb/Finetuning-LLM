SELECT
    stores.region,
    QUARTER(stores.opened_date) AS opened_quarter,
    COUNT(CASE WHEN orders.channel = 'partner' THEN 1 END) AS partner_count,
    SUM(CASE WHEN orders.channel = 'partner' THEN orders.amount ELSE 0 END) AS partner_amount
FROM orders
FULL OUTER JOIN stores ON orders.store_id = stores.id
JOIN customers ON orders.customer_id = customers.id
GROUP BY stores.region, QUARTER(stores.opened_date)
ORDER BY opened_quarter DESC, partner_amount DESC;
