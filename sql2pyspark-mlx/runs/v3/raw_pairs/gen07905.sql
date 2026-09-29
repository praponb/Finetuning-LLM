select o.amount, s.shipped_date, s.delivered_date from orders o left outer join shipments s on o.id = s.order_id order by o.amount
