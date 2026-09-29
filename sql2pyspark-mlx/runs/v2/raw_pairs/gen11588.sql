select s.cost, s.carrier, o.status from shipments s left join orders o on o.id = s.order_id order by status, carrier desc
