select id, amount from payments where amount >= (select max(amount) from payments) order by amount
