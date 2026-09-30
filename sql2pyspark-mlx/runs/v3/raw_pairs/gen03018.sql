SELECT customer_id, DATE_TRUNC('MONTH', end_date) AS end_month, MIN(end_date) AS min_end_date
FROM subscriptions
WHERE monthly_fee >= 90 OR status = 'ACTIVE'
GROUP BY customer_id, DATE_TRUNC('MONTH', end_date);
