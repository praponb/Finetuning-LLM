select t.id, t.amount, t.txn_time, t.merchant from transactions t where t.is_flagged = false and t.account_id in (select id from accounts where account_type not in ('SAVINGS', 'CREDIT'))
