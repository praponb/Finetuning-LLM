select a.id, a.account_id, a.amount from transactions a where a.amount = (select max(b.amount) from transactions b where b.account_id = a.account_id) order by a.amount
