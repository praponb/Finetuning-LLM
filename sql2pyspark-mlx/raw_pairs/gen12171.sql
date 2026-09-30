SELECT order_id, CASE WHEN amount >= 1390 THEN 'High' WHEN amount >= 470 THEN 'Medium' ELSE 'Low' END amount_tier
FROM payments
WHERE method <> 'paypal' AND paid_at >= '2023-05-01'
