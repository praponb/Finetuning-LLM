SELECT s.id, s.cost, s.delivered_date, o.order_date FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE o.discount IS NULL OR s.carrier IN ('FedEx', 'Kerry', 'DHL')
