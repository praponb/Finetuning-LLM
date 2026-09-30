SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, COUNT(CASE WHEN p.method = 'promptpay' THEN 1 END) AS promptpay_count
FROM payments p
LEFT JOIN orders o ON p.order_id = o.id
LEFT JOIN stores s ON o.store_id = s.id
GROUP BY DATE_TRUNC('MONTH', s.opened_date);
