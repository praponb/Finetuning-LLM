SELECT MONTH(opened_date) AS opened_month, SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count, MAX(balance) - MIN(balance) AS balance_range FROM accounts GROUP BY opened_month
