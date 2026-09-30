with with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.opened_date,
        latest.amount,
        latest.transaction_txn_time
    from accounts s
    left join lateral (
            select c.amount, c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
),
ranked as (
    -- QUALIFY-style window filter
    select
        id,
        opened_date,
        transaction_txn_time
    from with_last
    qualify row_number() over (partition by customer_id order by transaction_txn_time desc nulls last, id asc) = 1
),
with_last_accounts as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, top1.transaction_txn_time_2
    from ranked s
    left join lateral (
            select c.txn_time as transaction_txn_time_2
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.transaction_txn_time
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    -- Window metrics over several different partitions
    select *, row_number() over (order by opened_date desc, id asc) as row_seq
    from with_last_accounts
),
remaining_keys as (
    -- Set difference on keys
    select id
    from scored
    except
    select id
    from accounts
    where status in ('CLOSED', 'FROZEN', 'OPEN')
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from scored s
    inner join remaining_keys k on s.id = k.id
),
filtered as (
    -- Keep recent, valid rows
    select
        id,
        opened_date,
        transaction_txn_time,
        transaction_txn_time_2,
        round(row_seq * 0.85, 2) as row_seq_scaled
    from reconciled
    where to_date(transaction_txn_time_2) >= '2024-05-01'
)
select
    id,
    opened_date,
    transaction_txn_time,
    transaction_txn_time_2,
    row_seq_scaled
from filtered
order by id desc nulls last;
