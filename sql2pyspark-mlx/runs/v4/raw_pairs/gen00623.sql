select id, amount, status from payments where amount >= (select max(amount) from payments) order by id desc limit 10
