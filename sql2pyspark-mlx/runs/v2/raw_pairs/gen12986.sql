SELECT *
FROM accounts
WHERE opened_date BETWEEN '2023-03-10' AND '2024-05-01' AND (account_type IN ('CHECKING', 'SAVINGS') OR status = 'OPEN');
