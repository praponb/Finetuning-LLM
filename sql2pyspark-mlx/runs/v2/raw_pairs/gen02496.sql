SELECT
    status,
    SUM(shipping_fee) AS total_shipping_fee,
    COUNT(*) AS cnt,
    RANK() OVER (ORDER BY SUM(shipping_fee) DESC) AS shipping_fee_rank
FROM orders
WHERE customer_id IS NULL
GROUP BY status
ORDER BY shipping_fee_rank DESC
