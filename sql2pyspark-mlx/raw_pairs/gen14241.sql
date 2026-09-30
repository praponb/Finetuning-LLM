select s.delivered_date, s.order_id, o.shipping_fee, o.amount from shipments s join orders o on s.order_id = o.id
