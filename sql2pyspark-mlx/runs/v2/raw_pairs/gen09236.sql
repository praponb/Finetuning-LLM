SELECT
    p.paid_at,
    o.order_date,
    o.customer_id,
    c.name,
    ADD_MONTHS(o.order_date, 1) AS order_date_plus_months
FROM payments p
JOIN orders o ON o.id = p.order_id
JOIN customers c ON c.id = o.customer_id
WHERE c.country IN ('DE', 'TH') OR c.name LIKE 'Ca%'
ORDER BY name, customer_id;
