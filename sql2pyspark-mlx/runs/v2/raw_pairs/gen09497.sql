select p.id, p.order_id, p.method, p.amount, o.shipping_fee from payments p inner join orders o on p.order_id = o.id where p.paid_at >= '2024-03-01';
