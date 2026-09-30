SELECT account_type, MAX(balance) AS max_balance FROM accounts WHERE balance IS NOT NULL OR balance >= 77000 GROUP BY account_type;
