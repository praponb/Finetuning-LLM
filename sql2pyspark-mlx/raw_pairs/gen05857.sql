with cleaned as (
    -- Keep recent, valid rows
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        cast(is_flagged as int) as is_flagged_flag
    from transactions
    where txn_type in ('REFUND', 'FEE')
),
enriched as (
    -- Attach accounts attributes
    select s.*, b.status
    from cleaned s
    left join accounts b
        on s.account_id = b.id
),
base as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        trim(merchant) as clean_merchant,
        concat(merchant, ' ', txn_type) as merchant_txn_type
    from enriched
    where txn_time > to_date('2023-07-19')
      and merchant = 'Amazon'
),
enriched_transactions as (
    -- Attach accounts attributes
    select
        e.id,
        e.account_id,
        e.amount,
        e.is_flagged_flag,
        e.clean_merchant,
        p.balance,
        p.opened_date,
        p.status as account_status
    from base e
    left join accounts p
        on e.account_id = p.id
),
enriched_transactions_2 as (
    -- Window metrics over several different partitions
    select
        *,
        avg(amount) over (partition by account_id, account_status order by balance, id rows between 3 preceding and current row) as last4_avg_amount,
        count(amount) over (partition by account_id, account_status order by balance, id rows between 3 preceding and current row) as last4_count_amount,
        min(amount) over (partition by account_id, account_status order by balance, id rows between 3 preceding and current row) as last4_min_amount,
        lag(balance, 1, 0) over (order by balance, id) as prev_balance
    from enriched_transactions
    where account_id in (2, 18, 19, 30)
),
joined as (
    -- Attach accounts attributes
    select
        s.*,
        a.balance as account_balance,
        a.account_type
    from enriched_transactions_2 s
    inner join accounts a
        on s.account_id = a.id
),
enriched_transactions_3 as (
    -- Attach accounts attributes
    select
        a.*,
        d.status,
        d.opened_date as account_opened_date
    from joined a
    left join accounts d
        on a.account_id = d.id
)
-- Multi-level aggregation
select
    date_trunc('quarter', opened_date) as quarter_start,
    approx_percentile(account_balance, 0.5) as account_balance_p50,
    count(distinct id) as distinct_ids,
    round(avg(balance), 2) as avg_balance
from enriched_transactions_3
group by cube (date_trunc('quarter', opened_date))
order by avg_balance desc
