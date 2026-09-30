SELECT DATE_FORMAT(stores.opened_date, 'yyyy-MM') opened_month, SUM(CASE WHEN orders.channel = 'web' THEN orders.amount ELSE 0 END) web_amount, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(orders.channel))) channel_values
FROM orders
JOIN stores ON orders.store_id = stores.id
WHERE stores.region = 'North'
GROUP BY DATE_FORMAT(stores.opened_date, 'yyyy-MM')
