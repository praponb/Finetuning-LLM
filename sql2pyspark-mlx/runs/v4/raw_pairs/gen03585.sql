SELECT CASE WHEN channel IN ('store', 'web') THEN 'core' ELSE 'other' END AS channel_group, SUM(shipping_fee) AS total_shipping_fee, DENSE_RANK() OVER (ORDER BY SUM(shipping_fee) DESC) AS rnk
FROM orders
WHERE shipping_fee < 24
GROUP BY CASE WHEN channel IN ('store', 'web') THEN 'core' ELSE 'other' END
ORDER BY 2;
