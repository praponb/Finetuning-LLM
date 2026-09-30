select store_id from orders where amount > 1510 union all select store_id from orders where status = 'SHIPPED'
