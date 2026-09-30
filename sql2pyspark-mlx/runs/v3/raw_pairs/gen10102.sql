SELECT status, TRIM(status) AS clean_status FROM accounts WHERE YEAR(opened_date) >= 2023 AND balance >= 50000 ORDER BY clean_status, 1 DESC
