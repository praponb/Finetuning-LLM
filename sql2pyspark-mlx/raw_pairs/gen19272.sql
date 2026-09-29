with filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        last_day(opened_date) as opened_date_month_end,
        coalesce(status, 'OTHER') as status_filled
    from accounts
    where opened_date >= to_date('2023-03-28')
),
metrics as (
    -- Ranking and gap features
    select
        *,
        first_value(status) ignore nulls over (partition by account_type order by opened_date_month_end, id rows between unbounded preceding and current row) as first_known_status,
        lag(balance, 1, 0) over w_running as prev_balance
    from filtered
    window w_running as (partition by account_type, customer_id order by opened_date_month_end, id)
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, top1.transaction_txn_time
    from metrics s
    left join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        account_type,
        opened_date,
        opened_date_month_end,
        prev_balance,
        transaction_txn_time,
        ntile(5) over w_hist as prev_balance_bucket,
        dense_rank() over (partition by status order by prev_balance desc nulls first) as prev_balance_dense_rank,
        ntile(5) over (order by prev_balance, id) as prev_balance_bucket_2
    from with_last
    window w_hist as (partition by account_type order by prev_balance desc, id)
)
-- Multi-level aggregation
select
    case when prev_balance >= 67000 then 'HIGH' else 'LOW' end as prev_balance_tier,
    date_trunc('year', transaction_txn_time) as year_start,
    percentile_approx(prev_balance, array(0.25, 0.5, 0.75)) as prev_balance_quartiles,
    count(case when account_type = 'LOAN' then 1 end) as loan_count,
    count_if(account_type = 'CREDIT') as credit_events,
    max(prev_balance) as max_prev_balance,
    (unix_timestamp(max(transaction_txn_time)) - unix_timestamp(min(transaction_txn_time))) / 60 as span_minutes,
    round(avg(prev_balance), 2) as avg_prev_balance
from with_windows
group by 1, 2
order by credit_events desc
