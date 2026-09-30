SELECT status
FROM payments
WHERE status NOT IN ('REFUNDED', 'FAILED', 'SUCCESS')
UNION ALL
SELECT status
FROM shipments
WHERE carrier <> 'UPS';
