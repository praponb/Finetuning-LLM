SELECT account_type, balance, CASE WHEN status = 'OPEN' THEN 1 ELSE 0 END is_open FROM accounts WHERE balance < 69000 AND opened_date <= '2024-06-01';
