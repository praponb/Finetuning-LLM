SELECT YEAR(s.opened_date) opened_year, MAX(p.paid_at) latest_paid_at, SUM(CASE WHEN p.method = 'bank_transfer' THEN 1 ELSE 0 END) bank_transfer_count
FROM payments p
INNER JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY YEAR(s.opened_date)
ORDER BY bank_transfer_count ASC, opened_year ASC
LIMIT 50;
