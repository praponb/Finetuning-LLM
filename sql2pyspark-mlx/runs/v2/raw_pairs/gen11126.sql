select id, amount, order_id, method from payments where amount >= (select avg(amount) from payments where amount < 1170);
