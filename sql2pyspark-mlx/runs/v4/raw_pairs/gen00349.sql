SELECT shipped_date AS event_date
FROM shipments
WHERE carrier IN ('UPS', 'DHL')
UNION
SELECT opened_date AS event_date
FROM accounts
WHERE account_type IN ('CREDIT', 'LOAN')
