SELECT status, MAX(opened_date) AS latest_opened_date
FROM accounts
WHERE account_type IN ('SAVINGS', 'LOAN', 'CHECKING') AND opened_date > '2023-09-12'
GROUP BY status;
