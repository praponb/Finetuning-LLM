select id, amount, method, order_id from payments where amount >= (select max(amount) from payments)
