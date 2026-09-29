select id, amount from transactions where amount > (select max(amount) from transactions) order by id desc
