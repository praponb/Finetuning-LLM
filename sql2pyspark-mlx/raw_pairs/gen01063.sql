SELECT
    o.id,
    o.order_date,
    o.status,
    s.region,
    s.opened_date,
    c.credit_limit,
    c.name,
    CASE WHEN o.amount >= 1760 THEN 'High' WHEN o.amount >= 570 THEN 'Medium' ELSE 'Low' END AS amount_tier
FROM orders o
JOIN stores s ON s.id = o.store_id
LEFT OUTER JOIN customers c ON o.customer_id = c.id
ORDER BY region DESC;
