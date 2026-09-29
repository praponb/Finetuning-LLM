SELECT id, order_date, discount, discount - LAG(discount, 2) OVER (ORDER BY order_date, id) AS discount_change FROM orders WHERE channel IN ('store', 'partner', 'web');
