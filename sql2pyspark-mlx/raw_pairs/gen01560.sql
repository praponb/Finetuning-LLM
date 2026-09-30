SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC NULLS LAST, id ASC) AS rn
FROM accounts
QUALIFY rn = 1;
