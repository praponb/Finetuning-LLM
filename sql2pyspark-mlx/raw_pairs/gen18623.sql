SELECT s.id, s.status, o.order_date, DATE_ADD(s.shipped_date, 23) AS shipped_date_plus_23d FROM shipments s FULL OUTER JOIN orders o ON s.order_id = o.id
