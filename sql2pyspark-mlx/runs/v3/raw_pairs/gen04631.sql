SELECT signup_date
FROM customers
UNION
SELECT opened_date
FROM stores;
