SELECT CASE WHEN stores.sqft >= 16209 THEN 'high' WHEN stores.sqft >= 2734 THEN 'mid' ELSE 'low' END AS sqft_bucket, SORT_ARRAY(COLLECT_SET(orders.channel)) AS channel_list
FROM orders
INNER JOIN customers ON customers.id = orders.customer_id
LEFT OUTER JOIN stores ON stores.id = orders.store_id
WHERE orders.discount >= 0.16
GROUP BY CASE WHEN stores.sqft >= 16209 THEN 'high' WHEN stores.sqft >= 2734 THEN 'mid' ELSE 'low' END;
