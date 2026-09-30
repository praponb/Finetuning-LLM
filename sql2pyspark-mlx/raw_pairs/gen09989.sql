SELECT
    paid_at,
    method,
    amount,
    CASE WHEN amount >= 1330 THEN 'High' WHEN amount >= 670 THEN 'Medium' ELSE 'Low' END AS amount_level
FROM payments
ORDER BY amount DESC, method;
