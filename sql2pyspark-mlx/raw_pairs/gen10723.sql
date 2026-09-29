SELECT price, 'product' record_type
FROM products
UNION ALL
SELECT amount, 'payment' record_type
FROM payments
WHERE method != 'bank_transfer'
