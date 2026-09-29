SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date, id) rn FROM accounts WHERE status = 'FROZEN') t WHERE rn <= 2
