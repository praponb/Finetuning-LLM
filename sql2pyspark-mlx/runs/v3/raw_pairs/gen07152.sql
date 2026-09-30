SELECT
    status,
    MAX(order_date) AS latest_order_date,
    SUM(CASE WHEN channel = 'mobile' THEN amount ELSE 0 END) AS mobile_amount,
    MIN(shipping_fee) AS min_shipping_fee
FROM orders
WHERE MONTH(order_date) IN (2, 9, 12) AND (channel = 'mobile' OR customer_id IS NULL)
GROUP BY 1
