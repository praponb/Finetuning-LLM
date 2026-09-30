SELECT MONTH(stores.opened_date) AS opened_month, SUM(shipments.cost) AS total_cost, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(shipments.status))) AS status_values
FROM shipments
FULL OUTER JOIN orders ON shipments.order_id = orders.id
LEFT JOIN stores ON orders.store_id = stores.id
GROUP BY MONTH(stores.opened_date)
