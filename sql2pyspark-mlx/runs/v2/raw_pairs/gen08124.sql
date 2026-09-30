SELECT COUNT(CASE WHEN method = 'promptpay' THEN 1 END) promptpay_count
FROM payments
WHERE order_id IS NULL AND method IN ('cash', 'card', 'paypal')
ORDER BY promptpay_count DESC
