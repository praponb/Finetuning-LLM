SELECT balance, status FROM accounts WHERE balance BETWEEN 47000 AND 58000 AND (opened_date BETWEEN '2023-05-01' AND '2024-02-01' OR customer_id = 25) ORDER BY balance DESC;
