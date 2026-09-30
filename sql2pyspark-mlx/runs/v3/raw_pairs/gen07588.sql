SELECT s.delivered_date, o.amount, o.status, o.discount, LOWER(o.status) status_lower FROM shipments s JOIN orders o ON s.order_id = o.id WHERE s.carrier != 'UPS'
