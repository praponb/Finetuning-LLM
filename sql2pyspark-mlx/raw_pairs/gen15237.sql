with filtered as (
    -- Keep recent, valid rows
    select *, substring(account_type, 1, 3) account_type_prefix
    from accounts
    where status is distinct from 'FROZEN'
      and balance between 69000 and 77000
),
with_dims as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.balance,
        src.status,
        src.account_type_prefix,
        d.age,
        d.credit_limit,
        d.city
    from filtered src
    inner join customers d
        on src.customer_id = d.id
    where not d.is_active
),
latest_match as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.txn_time,
        top1.txn_type
    from with_dims s
    left join lateral (
            select c.txn_time, c.txn_type
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
        balance,
        age,
        city,
        txn_time,
        txn_type,
        lead(balance) over w_recent next_balance,
        lag(balance) over w_recent prev_balance,
        lead(credit_limit) over w_recent next_credit_limit,
        lag(credit_limit) over w_recent prev_credit_limit,
        row_number() over w_recent row_seq,
        sum(credit_limit) over (order by cast(txn_time as bigint) range between 7776000 preceding and current row) credit_limit_sum_90d
    from latest_match
    window w_recent as (partition by status, customer_id order by age, id)
)
select
    id,
    customer_id,
    balance,
    age,
    city,
    prev_balance,
    next_credit_limit,
    prev_credit_limit,
    row_seq
from with_windows
order by row_seq desc nulls first;
