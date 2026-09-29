SELECT
    id,
    amount,
    customer_id,
    status,
    CASE WHEN amount >= 1350 THEN 'High' WHEN amount >= 450 THEN 'Medium' ELSE 'Low' END AS amount_band
FROM orders
WHERE customer_id = 31 AND (amount <= 630 OR channel NOT IN ('partner', 'mobile', 'store'))
ORDER BY id DESC, amount DESC
