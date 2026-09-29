select s.* from accounts s where exists (select 1 from transactions ch where ch.account_id = s.id and ch.txn_time < s.opened_date)
