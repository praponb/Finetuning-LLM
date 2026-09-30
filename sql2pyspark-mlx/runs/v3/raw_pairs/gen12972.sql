SELECT YEAR(c.signup_date) AS signup_year, COUNT(DISTINCT o.shipping_fee) AS unique_statuss, SUM(CASE WHEN o.channel = 'partner' THEN o.amount ELSE 0 END) AS partner_amount
FROM orders o
RIGHT OUTER JOIN customers c ON o.customer_id = c.id
WHERE c.credit_limit > 14800
GROUP BY YEAR(c.signup_date)
ORDER BY partner_amount DESC, signup_year;
