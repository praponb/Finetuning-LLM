WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS rnk FROM accounts) SELECT id, account_type, opened_date, status FROM ranked WHERE rnk <= 5
