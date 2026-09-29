SELECT balance, account_type, status FROM accounts WHERE opened_date BETWEEN '2023-06-01' AND '2024-03-01' AND status = 'OPEN' ORDER BY status ASC, account_type ASC
