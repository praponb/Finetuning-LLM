SELECT opened_date FROM accounts WHERE account_type NOT IN ('SAVINGS', 'CREDIT', 'CHECKING') UNION SELECT launch_date FROM products ORDER BY opened_date
