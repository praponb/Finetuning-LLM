SELECT city, SUM(credit_limit) AS total_credit_limit, RANK() OVER (ORDER BY SUM(credit_limit) DESC) AS rnk FROM customers WHERE segment = 'SMB' GROUP BY city ORDER BY total_credit_limit DESC
