select s.order_id, s.status, o.amount from shipments s left join orders o on s.order_id = o.id where o.amount between 1390 and 1410 and s.carrier = 'FedEx';
