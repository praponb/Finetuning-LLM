select id, balance, status, opened_date, trim(account_type) as clean_account_type from accounts where opened_date < '2023-07-01' and (customer_id in (4, 6, 24) or opened_date > '2024-01-01');
