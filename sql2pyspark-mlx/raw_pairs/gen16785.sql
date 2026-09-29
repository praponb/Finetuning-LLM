with enriched as (
    select e.*, d.credit_limit
    from accounts e
    left join customers d
        on e.customer_id = d.id
),
latest_match as (
    select
        s.id,
        s.account_type,
        s.status,
        s.credit_limit,
        top1.txn_type
    from enriched s
    left join lateral (
            select c.txn_type
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
),
not_flagged as (
    select s.*
    from latest_match s
    where exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_type = 'FEE'
        )
),
with_last as (
    select
        s.id,
        s.account_type,
        s.txn_type,
        x.transaction_amount
    from not_flagged s
    left join lateral (
            select c.amount as transaction_amount
            from transactions c
            where c.account_id = s.id
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
)
select
    id,
    txn_type,
    transaction_amount,
    case when txn_type rlike '^(FEE|CREDIT)$' then 'GROUP_A' else 'GROUP_B' end as txn_type_zone
from with_last
