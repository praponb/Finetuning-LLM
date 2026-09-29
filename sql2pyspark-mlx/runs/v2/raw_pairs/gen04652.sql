SELECT orders.channel, COUNT(CASE WHEN payments.method = 'promptpay' THEN 1 END) promptpay_count, COUNT(payments.amount) amount_count
FROM payments
LEFT OUTER JOIN orders ON payments.order_id = orders.id
WHERE payments.order_id IS NULL
GROUP BY orders.channel;
