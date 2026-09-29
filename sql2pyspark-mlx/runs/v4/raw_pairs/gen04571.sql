SELECT revenue, 'store_sale' AS source
FROM store_sales
WHERE revenue BETWEEN 1400 AND 2800
UNION
SELECT amount, 'payment' AS source
FROM payments
WHERE amount > 630
ORDER BY source;
