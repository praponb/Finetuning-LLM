select t.id, t.txn_type from transactions t where t.txn_type not in ('DEBIT', 'REFUND') and t.account_id in (select id from accounts where balance <= 71000) order by txn_type, id limit 1
