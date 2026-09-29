SELECT customer_id, PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles, COUNT(*) AS num_rows FROM accounts GROUP BY customer_id ORDER BY customer_id;
