SELECT p.amount, o.discount, o.shipping_fee FROM payments p FULL OUTER JOIN orders o ON o.id = p.order_id;
