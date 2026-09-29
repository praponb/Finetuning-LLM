select c.country, c.age, o.shipping_fee, o.amount from customers c left outer join orders o on c.id = o.customer_id
