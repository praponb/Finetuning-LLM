SELECT status
FROM accounts
WHERE opened_date <= '2023-05-02'
UNION ALL
SELECT status
FROM payments
WHERE method = 'card'
