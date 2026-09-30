select id, amount, row_number() over (order by amount desc, id) as rn from transactions where txn_time < '2025-02-01'
