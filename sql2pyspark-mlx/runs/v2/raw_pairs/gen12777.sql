select s.id, s.cost, o.customer_id, o.order_date, floor(o.shipping_fee) as shipping_fee_whole from shipments s left join orders o on o.id = s.order_id order by s.cost desc, order_date, id limit 20
