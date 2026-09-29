select t.id, t.amount, t.txn_time from transactions t where t.merchant = 'Lazada' and t.account_id not in (select id from accounts where opened_date <= '2024-09-01') order by amount, id limit 10
