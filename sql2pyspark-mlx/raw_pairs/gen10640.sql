SELECT *, DATEDIFF(CURRENT_DATE(), paid_at) AS days_since_paid_at
FROM payments
ORDER BY method DESC;
