select transactions.id, transactions.txn_type from transactions where transactions.account_id not in (select id from accounts where status is not null) order by txn_type;
