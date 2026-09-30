select p.method, p.status, p.paid_at, o.shipping_fee from payments p right outer join orders o on o.id = p.order_id
