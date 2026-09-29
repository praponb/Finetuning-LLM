SELECT status, 'account' source
FROM accounts
UNION ALL
SELECT status, 'shipment' source
FROM shipments
WHERE delivered_date BETWEEN '2023-05-01' AND '2024-02-01';
