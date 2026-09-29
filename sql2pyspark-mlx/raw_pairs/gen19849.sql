SELECT MONTH(opened_date) AS opened_month, COUNT(DISTINCT balance) AS unique_statuss FROM accounts WHERE balance > 33000 GROUP BY MONTH(opened_date) ORDER BY opened_month DESC, unique_statuss;
