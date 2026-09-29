SELECT * FROM (SELECT QUARTER(end_date) AS end_quarter, status, monthly_fee FROM subscriptions)
PIVOT (AVG(monthly_fee) FOR status IN ('CANCELLED', 'PAUSED', 'EXPIRED'))
ORDER BY end_quarter;
