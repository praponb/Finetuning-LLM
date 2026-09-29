SELECT customer_id, DATE_FORMAT(end_date, 'yyyy-MM') end_month, SUM(CASE WHEN status = 'PAUSED' THEN monthly_fee ELSE 0 END) paused_monthly_fee
FROM subscriptions
WHERE MONTH(end_date) IN (2, 4, 12)
GROUP BY 1, 2
ORDER BY customer_id DESC;
