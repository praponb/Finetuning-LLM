SELECT
    s.city,
    MONTH(s.opened_date) AS opened_month,
    SUM(CASE WHEN o.status = 'COMPLETED' THEN o.amount ELSE 0 END) AS completed_amount,
    SUM(CASE WHEN o.status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count
FROM orders o
JOIN stores s ON o.store_id = s.id
WHERE s.opened_date >= '2024-02-01'
GROUP BY s.city, MONTH(s.opened_date);
