with cleaned as (
    -- Basic cleansing and defaults
    select
        *,
        cast(amount as int) as amount_int,
        cast(amount as int) as amount_int_2
    from transactions
    where is_flagged = true
),
flagged as (
    -- Flag rows that start a new session (gap > interval)
    select *, case when txn_time - lag(txn_time) over (partition by merchant order by txn_time asc, id asc) > interval '14' day then 1 else 0 end as is_new_burst
    from cleaned
),
sessionized as (
    -- Running sum of the flag = session number
    select
        id,
        account_id,
        amount,
        txn_time,
        merchant,
        amount_int,
        amount_int_2,
        sum(is_new_burst) over (partition by merchant order by txn_time asc, id asc rows between unbounded preceding and current row) as session_id
    from flagged
),
enriched as (
    -- Attach accounts attributes
    select
        a.id,
        a.account_id,
        a.amount,
        a.txn_time,
        a.amount_int_2,
        a.session_id,
        b.status,
        b.account_type
    from sessionized a
    left join accounts b
        on a.account_id = b.id
    where b.customer_id is null
),
joined as (
    -- Attach accounts attributes
    select
        a.id,
        a.account_id,
        a.amount,
        a.session_id,
        b.account_type as account_account_type,
        b.balance
    from enriched a
    left join accounts b
        on a.account_id = b.id
)
-- Join a dimension and aggregate
select
    dim.status,
    src.account_account_type,
    count(*) as rows_total,
    min(src.amount) as min_amount
from joined src
inner join accounts dim on src.account_id = dim.id
group by grouping sets ((dim.status, src.account_account_type), (dim.status), ());
