select id, amount from payments where amount < (select min(amount) from payments) order by id desc
