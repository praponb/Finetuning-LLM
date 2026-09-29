SELECT YEAR(paid_at) AS paid_year, SUM(amount) AS total_amount, COUNT(*) AS cnt, DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM payments GROUP BY YEAR(paid_at);
