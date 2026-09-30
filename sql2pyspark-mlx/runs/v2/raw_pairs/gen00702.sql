SELECT MONTH(s.opened_date) AS opened_month, SORT_ARRAY(COLLECT_SET(o.channel)) AS channel_list
FROM orders o
FULL OUTER JOIN customers c ON o.customer_id = c.id
INNER JOIN stores s ON s.id = o.store_id
GROUP BY MONTH(s.opened_date)
ORDER BY opened_month DESC
