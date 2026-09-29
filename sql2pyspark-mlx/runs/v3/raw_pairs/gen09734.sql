SELECT id, paid_at, status
FROM payments
WHERE NOT (status IN ('FAILED', 'SUCCESS') AND method IS NOT NULL);
