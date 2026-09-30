SELECT c.segment, SUM(CASE WHEN o.channel = 'store' THEN o.discount ELSE 0 END) AS store_discount, SUM(CASE WHEN o.status = 'SHIPPED' THEN o.shipping_fee ELSE 0 END) AS shipped_shipping_fee
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY c.segment
HAVING SUM(CASE WHEN o.channel = 'store' THEN o.discount ELSE 0 END) >= 29
ORDER BY c.segment DESC, shipped_shipping_fee DESC;
