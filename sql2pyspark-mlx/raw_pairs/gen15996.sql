SELECT TO_DATE(paid_at) AS paid_day, SUM(amount) AS total_amount, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM payments GROUP BY TO_DATE(paid_at) ORDER BY rnk DESC;
