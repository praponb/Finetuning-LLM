SELECT
    order_id,
    amount,
    CASE WHEN status IN ('SUCCESS', 'FAILED') THEN 'Core' ELSE 'Other' END AS status_group,
    DAYOFWEEK(paid_at) AS paid_at_dayofweek
FROM payments
WHERE amount BETWEEN 490 AND 930 OR order_id IS NOT NULL;
