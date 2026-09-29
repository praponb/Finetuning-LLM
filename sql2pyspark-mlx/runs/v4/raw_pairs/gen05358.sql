with valid as (
    select
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        trim(merchant) as clean_merchant
    from transactions
    where amount > 2700
),
enriched as (
    select src.*, a.status
    from valid src
    left join accounts a
        on src.account_id = a.id
),
with_prev as (
    select *, lag(txn_time) over (partition by merchant order by txn_time, id) as prev_txn_time
    from enriched
),
with_gaps as (
    select *, case when prev_txn_time is null or txn_time > prev_txn_time + interval '14' day then 1 else 0 end as session_start_flag
    from with_prev
),
sessionized as (
    select *, sum(session_start_flag) over (partition by merchant order by txn_time, id rows between unbounded preceding and current row) as session_id
    from with_gaps
),
joined as (
    select
        src.*,
        d.balance,
        d.account_type
    from sessionized src
    left join accounts d
        on src.account_id = d.id
),
pivoted as (
    select *
    from (
        select
            txn_type,
            account_type,
            amount
        from joined
    ) src
    pivot (
        max(amount) as total,
        count(1) as item_count
        for account_type in ('SAVINGS', 'LOAN')
    )
)
select
    txn_type,
    SAVINGS_total,
    SAVINGS_item_count,
    LOAN_total
from pivoted
order by txn_type desc;
