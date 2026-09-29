SELECT order_id, method, MAX(amount) AS max_amount
FROM payments
WHERE method NOT IN ('promptpay', 'card', 'paypal') OR order_id IS NOT NULL
GROUP BY 1, 2
HAVING max_amount >= 840;
