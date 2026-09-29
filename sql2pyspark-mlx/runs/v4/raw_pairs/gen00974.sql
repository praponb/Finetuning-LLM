SELECT COUNT(*) num_accounts, ROUND(STDDEV(balance), 2) stddev_balance FROM accounts WHERE NOT (opened_date <= '2025-01-01' AND account_type NOT IN ('CHECKING', 'SAVINGS'));
