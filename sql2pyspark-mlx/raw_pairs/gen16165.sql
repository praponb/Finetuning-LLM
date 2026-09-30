SELECT s.region, ROUND(STDDEV(o.shipping_fee), 2) stddev_shipping_fee
FROM orders o
JOIN stores s ON o.store_id = s.id
GROUP BY s.region
HAVING ROUND(STDDEV(o.shipping_fee), 2) >= 48
ORDER BY 1;
