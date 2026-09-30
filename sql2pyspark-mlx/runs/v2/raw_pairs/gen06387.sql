select a.id, a.account_id, a.amount from transactions a where a.amount > (select avg(b.amount) from transactions b where b.account_id = a.account_id) order by a.amount desc, a.account_id desc;
