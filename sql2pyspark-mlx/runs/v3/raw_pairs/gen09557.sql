SELECT *, CAST(paid_at AS DATE) AS paid_at_day, CASE WHEN amount >= 1490 THEN 'High' WHEN amount >= 630 THEN 'Medium' ELSE 'Low' END AS amount_band
FROM payments
WHERE NOT (status = 'REFUNDED' AND status = 'FAILED')
ORDER BY order_id DESC, method
