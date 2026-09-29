SELECT COUNT(DISTINCT discount) AS unique_stores, SUM(CASE WHEN status = 'CANCELLED' THEN shipping_fee ELSE 0 END) AS cancelled_shipping_fee, COUNT(*) AS cnt
FROM orders
WHERE discount <= 0.38 AND (shipping_fee > 37 OR store_id IS NULL)
ORDER BY cnt DESC;
