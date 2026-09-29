SELECT id, opened_date FROM accounts WHERE opened_date = (SELECT MIN(opened_date) FROM accounts) ORDER BY 1 DESC;
