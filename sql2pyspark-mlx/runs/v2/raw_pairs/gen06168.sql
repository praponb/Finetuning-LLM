SELECT p.order_id, o.order_date, SUBSTRING(p.method, 1, 4) method_prefix FROM payments p LEFT OUTER JOIN orders o ON p.order_id = o.id ORDER BY method_prefix, order_date DESC
