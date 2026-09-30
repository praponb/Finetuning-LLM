SELECT *, TO_DATE(paid_at) AS paid_at_date, MONTH(paid_at) AS paid_at_month FROM payments WHERE TO_DATE(paid_at) < '2024-11-01'
