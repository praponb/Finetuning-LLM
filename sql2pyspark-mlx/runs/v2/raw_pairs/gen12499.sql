select o.status, count(distinct oi.unit_price) as unique_products from order_items oi inner join orders o on oi.order_id = o.id group by o.status order by unique_products
