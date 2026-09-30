SELECT *, YEAR(paid_at) AS paid_at_year
FROM payments
WHERE paid_at < '2024-06-01';
