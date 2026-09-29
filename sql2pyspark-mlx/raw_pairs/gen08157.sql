SELECT method, SUM(amount) AS total_amount, RANK() OVER (ORDER BY SUM(amount) DESC) AS amount_rank FROM payments GROUP BY method;
