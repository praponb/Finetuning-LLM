SELECT amount, CASE WHEN status IN ('COMPLETED', 'CANCELLED') THEN 'Core' ELSE 'Other' END AS status_group, ROUND(amount, 2) AS amount_rounded
FROM orders
