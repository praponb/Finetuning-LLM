SELECT
    id,
    order_id,
    status,
    INITCAP(TRIM(method)) AS method_title
FROM payments
WHERE method IN ('paypal', 'cash', 'bank_transfer') OR method IN ('card', 'promptpay', 'bank_transfer')
