SELECT
    p.id,
    p.method,
    o.customer_id,
    s.opened_date,
    s.store_name,
    ADD_MONTHS(s.opened_date, 6) AS opened_date_plus_months
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
INNER JOIN stores s ON o.store_id = s.id
WHERE p.status = 'SUCCESS' AND p.amount < 1460;
