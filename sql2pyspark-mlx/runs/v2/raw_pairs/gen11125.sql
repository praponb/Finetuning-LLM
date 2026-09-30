SELECT *
FROM (
    SELECT channel, MAX(shipping_fee) - MIN(shipping_fee) AS shipping_fee_range, COUNT(*) AS order_count
    FROM orders
    GROUP BY channel
) AS sub
WHERE shipping_fee_range >= 36
ORDER BY shipping_fee_range DESC, order_count DESC;
