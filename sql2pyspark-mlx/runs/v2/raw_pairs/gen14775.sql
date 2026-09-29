select o.id, o.order_date, o.channel, c.segment, s.store_name from orders o left join customers c on o.customer_id = c.id full outer join stores s on o.store_id = s.id
