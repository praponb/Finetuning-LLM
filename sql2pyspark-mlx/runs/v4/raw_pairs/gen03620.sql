select p.order_id, p.method, o.shipping_fee, o.customer_id from payments p inner join orders o on p.order_id = o.id where p.method in ('bank_transfer', 'cash', 'promptpay');
