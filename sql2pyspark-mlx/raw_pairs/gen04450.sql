SELECT
    c.segment,
    src.status,
    COUNT(*) AS cnt,
    MAX(src.monthly_fee) AS max_monthly_fee
FROM subscriptions src
JOIN customers c ON src.customer_id = c.id
GROUP BY c.segment, src.status
ORDER BY cnt DESC NULLS LAST;
