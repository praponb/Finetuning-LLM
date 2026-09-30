SELECT shipped_date
FROM shipments
WHERE delivered_date BETWEEN '2025-03-01' AND '2025-08-28'
EXCEPT
SELECT opened_date
FROM accounts
WHERE account_type NOT IN ('SAVINGS', 'LOAN')
