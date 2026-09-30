with valid as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        concat(account_type, '/', status) as account_type_status,
        lower(status) as status_lower
    from accounts
    where status is not null
      and status is not null
),
with_latest as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.customer_id,
        s.account_type,
        s.balance,
        s.status_lower,
        top1.transaction_amount,
        top1.txn_type
    from valid s
    join lateral (
            select c.amount as transaction_amount, c.txn_type
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) top1 on true
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_latest s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and not ch.is_flagged
        )
),
valid_accounts as (
    -- Basic cleansing and defaults
    select
        *,
        abs(balance) as abs_balance,
        coalesce(txn_type, 'DIRECT') as resolved_txn_type
    from kept
    where transaction_amount > 3400
),
matched_keys as (
    -- Set difference on keys
    select customer_id
    from valid_accounts
    except distinct
    select customer_id
    from accounts
    where account_type != 'SAVINGS'
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid_accounts s
    inner join matched_keys k on s.customer_id = k.customer_id
)
-- Native SQL UNPIVOT (wide to long)
select *
from (
    select
        account_type,
        abs_balance,
        balance,
        transaction_amount
    from kept_rows
) src
unpivot (
    value for measure in (abs_balance, balance, transaction_amount)
) long_rows
