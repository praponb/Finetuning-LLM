select t.id, t.txn_time, t.merchant, t.account_id from transactions t where t.account_id in (select id from accounts where opened_date < '2023-07-01');
