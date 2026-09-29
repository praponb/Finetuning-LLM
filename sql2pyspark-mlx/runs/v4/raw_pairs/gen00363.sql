with with_gaps as (
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval 14 days then 1 else 0 end as new_group_flag
    from transactions
),
sessionized as (
    select *, sum(new_group_flag) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as island_id
    from with_gaps
)
select
    merchant,
    count(case when txn_type = 'CREDIT' then 1 end) as credit_count,
    min(txn_time) as first_txn_time,
    sum(case when merchant = 'Lazada' then island_id else 0 end) as lazada_island_id
from sessionized
group by merchant
order by credit_count desc nulls last;
