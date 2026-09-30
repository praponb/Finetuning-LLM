with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        lower(account_type) as account_type_lower
    from accounts
    where opened_date > date '2023-09-21'
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        b.credit_limit,
        b.name
    from cleaned s
    left join customers b
        on s.customer_id = b.id
),
latest as (
    -- QUALIFY-style window filter
    select *
    from enriched
    qualify row_number() over (partition by customer_id, account_type order by balance, id) = 1
),
with_latest as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, x.transaction_txn_time
    from latest s
    join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
with_last as (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    select s.*, latest.transaction_txn_type
    from with_latest s
    left join lateral (
            select c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) latest on true
)
select
    id,
    account_type,
    balance,
    opened_date,
    status,
    account_type_lower,
    name,
    transaction_txn_time,
    case when account_type rlike '^(SAVINGS|CREDIT)$' then 'CORE' else 'GROUP_A' end as account_type_zone
from with_last
