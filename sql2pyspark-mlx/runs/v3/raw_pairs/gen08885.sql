SELECT
    DATE_TRUNC('MONTH', s.opened_date) opened_month,
    YEAR(s.opened_date) opened_year,
    AVG(p.amount) avg_amount,
    COUNT(p.amount) amount_count
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
RIGHT JOIN stores s ON o.store_id = s.id
WHERE o.customer_id = 37
GROUP BY DATE_TRUNC('MONTH', s.opened_date), YEAR(s.opened_date);
