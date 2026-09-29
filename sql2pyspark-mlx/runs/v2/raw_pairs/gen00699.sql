select a.id, a.account_type from accounts a where a.id in (select account_id from transactions where txn_type in ('REFUND', 'FEE'));
