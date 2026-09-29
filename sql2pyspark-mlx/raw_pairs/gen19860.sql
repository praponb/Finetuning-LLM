select s.id, s.delivered_date, s.cost, o.shipping_fee, o.amount, o.status from shipments s join orders o on s.order_id = o.id order by cost desc;
