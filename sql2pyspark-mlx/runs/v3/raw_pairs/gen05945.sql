SELECT MONTH(paid_at) AS paid_month, COUNT(DISTINCT amount) AS unique_orders FROM payments GROUP BY 1;
