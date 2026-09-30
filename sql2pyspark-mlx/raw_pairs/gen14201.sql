select a.id, a.opened_date from accounts a where not exists (select 1 from transactions t where t.account_id = a.id and t.txn_type in ('DEBIT', 'FEE', 'REFUND'));
