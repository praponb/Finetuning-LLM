select id, discount from orders where discount > (select max(discount) from orders);
