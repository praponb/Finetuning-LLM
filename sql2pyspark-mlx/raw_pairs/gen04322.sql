SELECT id, account_type, balance FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance DESC, id) rn FROM accounts) ranked WHERE rn = 1 ORDER BY account_type;
