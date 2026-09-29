select cost, delivered_date, order_id from shipments where not (order_id in (8, 29) or delivered_date between '2025-04-01' and '2025-06-01') order by order_id desc
