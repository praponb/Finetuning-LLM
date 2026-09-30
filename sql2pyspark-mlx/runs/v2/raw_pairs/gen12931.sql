select accounts.id, accounts.customer_id from accounts where accounts.id in (select account_id from transactions where txn_time >= '2024-06-01') order by id asc limit 10
