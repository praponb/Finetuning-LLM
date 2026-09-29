select t.id, t.txn_time, t.account_id, t.is_flagged, a.balance from transactions t inner join accounts a on a.id = t.account_id;
