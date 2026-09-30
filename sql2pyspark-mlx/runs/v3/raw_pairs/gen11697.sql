select id, amount from transactions where amount = (select min(amount) from transactions where hour(txn_time) between 6 and 14) order by id, amount desc
