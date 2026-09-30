SELECT
    stores.region,
    CASE WHEN stores.sqft >= 12755 THEN 'high' WHEN stores.sqft >= 4331 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    MAX(payments.amount) AS max_amount,
    SORT_ARRAY(COLLECT_SET(payments.method)) AS method_list
FROM payments
INNER JOIN orders ON orders.id = payments.order_id
LEFT JOIN stores ON stores.id = orders.store_id
WHERE payments.status = 'FAILED'
GROUP BY stores.region, CASE WHEN stores.sqft >= 12755 THEN 'high' WHEN stores.sqft >= 4331 THEN 'mid' ELSE 'low' END
ORDER BY max_amount, region;
