with cleaned as (
    select
        *,
        date_trunc('year', txn_time) as txn_time_period,
        to_date(txn_time) as txn_time_date,
        coalesce(merchant, 'DIRECT') as resolved_merchant
    from transactions
    where txn_time >= to_date('2023-05-25')
      and merchant is not null
),
enriched as (
    select
        e.id,
        e.account_id,
        e.txn_time,
        e.txn_time_date,
        a.status,
        a.balance,
        a.account_type
    from cleaned e
    left join accounts a
        on e.account_id = a.id
    where a.account_type = 'LOAN'
),
with_dims as (
    select
        a.id,
        a.account_id,
        a.txn_time_date,
        a.status,
        p2.balance as account_balance,
        p2.status as account_status,
        p2.opened_date
    from enriched a
    join accounts p2
        on a.account_id = p2.id
),
scored as (
    select
        *,
        count(account_balance) over (partition by account_status) as account_status_count_account_balance,
        avg(account_balance) over (partition by year(txn_time_date), account_status order by opened_date, id rows between 3 preceding and 1 preceding) as account_balance_avg_3_prior,
        percent_rank() over (order by account_balance nulls last) as account_balance_percent_rank,
        lead(account_balance, 1) over (partition by month(opened_date) order by opened_date, id) as next_account_balance
    from with_dims
),
latest as (
    select *
    from (
        select *, row_number() over (partition by status order by next_account_balance, id) as dedup_rank
        from scored
    ) ranked
    where dedup_rank = 1
)
select
    date_trunc('week', txn_time_date) as week_start,
    count(distinct account_id) as distinct_account_ids,
    size(collect_set(status)) as num_status,
    approx_percentile(account_balance, 0.95) as account_balance_p95,
    round(avg(account_balance), 2) as avg_account_balance
from latest
group by week_start
order by week_start nulls first;
