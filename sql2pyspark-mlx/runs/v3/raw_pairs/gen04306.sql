SELECT
    store_id,
    status,
    MAX(discount) - MIN(discount) discount_range,
    COUNT(discount) discount_count,
    MAX(shipping_fee) - MIN(shipping_fee) shipping_fee_range
FROM orders
WHERE discount BETWEEN 0.23 AND 0.25 AND channel IN ('web', 'mobile')
GROUP BY store_id, status
HAVING MAX(discount) - MIN(discount) >= 67
ORDER BY discount_range DESC
