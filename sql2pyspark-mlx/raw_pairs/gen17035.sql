select t.id, t.account_id, t.amount from transactions t where t.amount > (select avg(t2.amount) from transactions t2 where t2.account_id = t.account_id) order by t.amount, t.account_id desc
