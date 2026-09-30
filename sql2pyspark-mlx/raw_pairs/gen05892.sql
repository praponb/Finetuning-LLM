SELECT QUARTER(opened_date) AS opened_quarter, customer_id, SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_count FROM accounts GROUP BY ROLLUP(QUARTER(opened_date), customer_id)
