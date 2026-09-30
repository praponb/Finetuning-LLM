select s.cost, s.order_id, s.carrier, o.order_date from shipments s join orders o on s.order_id = o.id where o.amount > 540 or s.carrier in ('DHL', 'FedEx', 'UPS') order by s.carrier, s.cost desc
