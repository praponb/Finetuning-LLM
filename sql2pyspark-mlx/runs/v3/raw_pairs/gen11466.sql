SELECT status, 'account' AS source
FROM accounts
WHERE account_type NOT IN ('LOAN', 'CHECKING')
UNION
SELECT status, 'shipment' AS source
FROM shipments;
