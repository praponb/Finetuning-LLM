select
    a.opened_date,
    t.txn_time,
    t.merchant,
    cast(t.txn_time as date) as txn_time_day
from accounts a
left outer join transactions t on t.account_id = a.id
where t.txn_time >= '2023-03-10' or t.amount between 1500 and 2300
order by txn_time_day
