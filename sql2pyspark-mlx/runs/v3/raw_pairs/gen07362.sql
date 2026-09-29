SELECT
    method,
    CASE WHEN amount >= 1490 THEN 'high' WHEN amount >= 290 THEN 'mid' ELSE 'low' END AS amount_bucket,
    CASE WHEN status IN ('FAILED', 'SUCCESS') THEN 'tier_1' ELSE 'other' END AS status_group,
    ROUND(STDDEV(amount), 2) AS stddev_amount,
    AVG(amount) AS avg_amount
FROM payments
WHERE HOUR(paid_at) BETWEEN 9 AND 14
GROUP BY method, CASE WHEN amount >= 1490 THEN 'high' WHEN amount >= 290 THEN 'mid' ELSE 'low' END, CASE WHEN status IN ('FAILED', 'SUCCESS') THEN 'tier_1' ELSE 'other' END;
