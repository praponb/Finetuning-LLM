SELECT DATE_TRUNC('MONTH', opened_date) AS opened_month, COUNT(*) AS cnt
FROM stores
WHERE sqft <= 11540 AND city = 'Bangkok'
GROUP BY opened_month;
