select oi.id, oi.quantity, oi.unit_price, o.amount, p.supplier_id, p.category from order_items oi inner join orders o on oi.order_id = o.id right outer join products p on p.id = oi.product_id
