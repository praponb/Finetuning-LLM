SELECT
    status,
    CASE WHEN amount >= 1250 THEN 'high' WHEN amount >= 530 THEN 'mid' ELSE 'low' END AS amount_bucket,
    CASE WHEN status IN ('FAILED', 'SUCCESS') THEN 'priority' ELSE 'other' END AS status_group,
    COUNT(*) AS num_payments
FROM payments
GROUP BY 1, 2, 3;
