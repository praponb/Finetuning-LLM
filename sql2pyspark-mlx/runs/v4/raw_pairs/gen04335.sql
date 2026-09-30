SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY balance, id) rn FROM accounts WHERE customer_id IS NOT NULL) r WHERE rn <= 2 ORDER BY status ASC, opened_date ASC
