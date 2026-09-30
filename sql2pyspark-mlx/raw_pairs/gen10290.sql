with base as (
    -- Keep recent, valid rows
    select
        *,
        case when amount >= 4000 then 'High' when amount >= 1200 then 'Medium' else 'Low' end as amount_band,
        hour(txn_time) as txn_time_hour,
        coalesce(merchant, 'OTHER') as merchant_clean
    from transactions
    where merchant is not null
      and to_date(txn_time) < '2025-07-18'
),
with_gaps as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by account_id order by txn_time, id) > interval '7' day then 1 else 0 end as is_new_session
    from base
),
islands as (
    -- Running sum of the flag = session number
    select
        id,
        account_id,
        txn_time,
        is_flagged,
        sum(is_new_session) over (partition by account_id order by txn_time, id rows between unbounded preceding and current row) as burst_id
    from with_gaps
),
joined as (
    -- Attach accounts attributes
    select e.*, p.account_type
    from islands e
    left join accounts p
        on e.account_id = p.id
),
with_dims as (
    -- Attach accounts attributes
    select
        a.id,
        a.account_id,
        a.account_type,
        b.account_type as account_account_type
    from joined a
    left join accounts b
        on a.account_id = b.id
),
with_dims_transactions as (
    -- Attach accounts attributes
    select
        a.*,
        b.balance,
        b.status
    from with_dims a
    left join accounts b
        on a.account_id = b.id
    where b.opened_date <= '2023-08-01'
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        sum(balance) over w_running as running_sum_balance,
        min(balance) over w_running as running_min_balance,
        max(balance) over w_running as running_max_balance
    from with_dims_transactions
    window w_running as (partition by account_id order by balance, id rows between unbounded preceding and current row)
),
joined_transactions as (
    -- Attach accounts attributes
    select
        s.id,
        s.account_id,
        s.account_type,
        s.balance,
        s.running_sum_balance,
        a.status as account_status,
        a.balance as account_balance,
        a.opened_date
    from enriched s
    join accounts a
        on s.account_id = a.id
),
grouped as (
    -- Grouped metrics
    select
        account_id,
        avg(running_sum_balance) as avg_running_sum_balance,
        percentile_approx(running_sum_balance, array(0.25, 0.5, 0.75)) as running_sum_balance_quartiles,
        approx_percentile(running_sum_balance, array(0.25, 0.5, 0.75)) as running_sum_balance_quartiles_2,
        count(*) as row_count,
        round(sum(account_balance) / nullif(count(case when account_type = 'LOAN' then 1 end), 0), 2) as account_balance_per_loan,
        max(account_balance) as max_account_balance
    from joined_transactions
    group by 1
)
select
    account_id,
    avg_running_sum_balance,
    running_sum_balance_quartiles,
    running_sum_balance_quartiles_2
from grouped
order by account_id desc
limit 10
