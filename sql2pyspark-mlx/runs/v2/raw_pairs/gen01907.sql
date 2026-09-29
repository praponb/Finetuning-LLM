with src as (
    select *
    from transactions
    where txn_time < '2024-12-21'
),
txn_type_summary as (
    select txn_type, count(case when merchant = 'Shell' then 1 end) as shell_count
    from src
    group by txn_type
)
select txn_type, shell_count
from txn_type_summary
order by shell_count desc, txn_type
limit 3;
