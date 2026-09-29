SELECT customer_id, SUM(monthly_fee) AS total_monthly_fee, ROUND(SUM(monthly_fee) * 100.0 / SUM(SUM(monthly_fee)) OVER (), 2) AS pct_of_total
FROM subscriptions
WHERE monthly_fee BETWEEN 90 AND 180
GROUP BY customer_id;
