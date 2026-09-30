with joined as (
    select
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.txn_time,
        a.is_flagged,
        b.opened_date,
        b.status
    from transactions a
    left join accounts b
        on a.account_id = b.id
),
joined_transactions as (
    select
        a.*,
        d.account_type,
        d.balance,
        d.opened_date as account_opened_date
    from joined a
    left join accounts d
        on a.account_id = d.id
    where year(d.opened_date) = 2023
),
joined_transactions_2 as (
    select s.*, b.opened_date as account_opened_date_2
    from joined_transactions s
    join accounts b
        on s.account_id = b.id
),
with_gaps as (
    select *, case when lag(txn_time) over (partition by status order by txn_time asc, id asc) is null
                 or txn_time > lag(txn_time) over (partition by status order by txn_time asc, id asc) + interval 7 days then 1 else 0 end as new_group_flag
    from joined_transactions_2
),
sessionized as (
    select
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        status,
        balance,
        account_opened_date,
        account_opened_date_2,
        sum(new_group_flag) over (partition by status order by txn_time asc, id asc rows between unbounded preceding and current row) as burst_id
    from with_gaps
)
select
    date_trunc('year', txn_time) as year_start,
    sum(balance) filter (where txn_type = 'DEBIT') as debit_total_balance,
    percentile_approx(burst_id, array(0.25, 0.5, 0.75)) as burst_id_quartiles,
    min(txn_time) as first_seen
from sessionized
group by rollup (date_trunc('year', txn_time))
order by year_start desc nulls last
