SELECT
    id,
    amount,
    paid_at,
    CASE WHEN status IN ('SUCCESS', 'REFUNDED') THEN 'Tier 1' ELSE 'Other' END AS status_group
FROM payments
WHERE method IN ('card', 'bank_transfer', 'paypal');
