SELECT o.status, COUNT(CASE WHEN p.method = 'promptpay' THEN 1 END) AS promptpay_count FROM payments p INNER JOIN orders o ON p.order_id = o.id GROUP BY o.status ORDER BY promptpay_count DESC
