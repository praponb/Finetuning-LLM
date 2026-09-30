SELECT price
FROM products
INTERSECT
SELECT amount
FROM transactions
WHERE txn_type IN ('CREDIT', 'REFUND', 'FEE');
