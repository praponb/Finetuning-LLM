SELECT id, order_id, CASE WHEN amount >= 1670 THEN 'High' WHEN amount >= 660 THEN 'Medium' ELSE 'Low' END AS amount_band
FROM payments
WHERE amount BETWEEN 530 AND 1440 AND status NOT IN ('FAILED', 'SUCCESS');
