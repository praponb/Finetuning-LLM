SELECT p.id, p.method, o.order_date, o.channel, TO_DATE(p.paid_at) AS paid_at_date FROM payments p FULL OUTER JOIN orders o ON p.order_id = o.id ORDER BY paid_at_date;
