select s.id, s.delivered_date, s.carrier, o.status, o.customer_id from shipments s left join orders o on s.order_id = o.id where o.discount <= 0.19 order by s.carrier desc
