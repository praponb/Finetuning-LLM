SELECT status
FROM accounts
WHERE opened_date <= '2023-06-16'
UNION ALL
SELECT status
FROM shipments
WHERE carrier IN ('FedEx', 'UPS', 'Flash');
