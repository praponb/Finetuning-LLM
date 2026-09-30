select a.id, a.txn_type, a.amount, a.merchant from transactions a where a.amount > (select avg(b.amount) from transactions b where b.txn_type = a.txn_type);
