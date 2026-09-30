SELECT DATE_TRUNC('MONTH', s.opened_date) AS opened_month, MAX(o.amount) - MIN(o.amount) AS amount_range FROM orders o JOIN stores s ON s.id = o.store_id GROUP BY DATE_TRUNC('MONTH', s.opened_date);
