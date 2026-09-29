SELECT CASE WHEN c.credit_limit >= 17400 THEN 'high' WHEN c.credit_limit >= 5900 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, AVG(s.monthly_fee) AS avg_monthly_fee, SUM(CASE WHEN s.status = 'EXPIRED' THEN s.monthly_fee ELSE 0 END) AS expired_monthly_fee
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
GROUP BY CASE WHEN c.credit_limit >= 17400 THEN 'high' WHEN c.credit_limit >= 5900 THEN 'mid' ELSE 'low' END;
