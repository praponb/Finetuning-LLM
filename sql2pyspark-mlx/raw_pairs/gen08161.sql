with valid as (
    -- Filter the raw rows and derive helper columns
    select *, round(amount, 1) as amount_rounded
    from transactions
    where merchant is not null
      and txn_time >= to_timestamp('2023-05-05 19:00:00')
),
joined as (
    -- Attach accounts attributes
    select
        s.id,
        s.account_id,
        s.merchant,
        s.amount_rounded,
        a.status,
        a.account_type,
        a.balance
    from valid s
    inner join accounts a
        on s.account_id = a.id
    where a.status = 'OPEN'
),
matched_keys as (
    -- Set difference on keys
    select merchant, account_id
    from joined
    except distinct
    select merchant, account_id
    from transactions
    where txn_time >= '2025-03-01'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from joined s
    inner join matched_keys k on s.merchant = k.merchant and s.account_id = k.account_id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        cume_dist() over (order by balance desc) as balance_cume_dist,
        percent_rank() over (order by balance) as balance_percent_rank
    from verified
)
select
    id,
    merchant,
    amount_rounded,
    status,
    account_type,
    balance_percent_rank,
    case when account_type rlike '^(SAVINGS|CHECKING)$' then 'PRIORITY' else 'GROUP_A' end as account_type_zone
from scored;
