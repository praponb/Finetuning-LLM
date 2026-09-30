SELECT monthly_fee
FROM subscriptions
WHERE monthly_fee IS NOT NULL
EXCEPT
SELECT revenue AS monthly_fee
FROM store_sales
WHERE revenue IS NOT NULL;
