with ranked as (
    select *
    from accounts
    qualify row_number() over (partition by customer_id order by balance nulls last, id) = 1
),
eligible as (
    select s.*
    from ranked s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.account_id in (8, 22, 30, 31)
        )
),
latest_match as (
    select s.*, x.transaction_txn_type
    from eligible s
    left join lateral (
            select c.txn_type as transaction_txn_type
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
            order by c.txn_time desc, c.id desc
            limit 1
        ) x on true
),
eligible_accounts as (
    select s.*
    from latest_match s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.is_flagged
        )
),
latest_match_accounts as (
    select
        s.id,
        s.balance,
        top1.transaction_txn_time
    from eligible_accounts s
    left join lateral (
            select c.txn_time as transaction_txn_time
            from transactions c
            where c.account_id = s.id
            order by c.amount desc, c.id desc
            limit 1
        ) top1 on true
),
cleaned as (
    select *, datediff(current_date(), transaction_txn_time) days_since_transaction_txn_time
    from latest_match_accounts
    where transaction_txn_time >= '2024-11-01'
)
select
    date_trunc('year', transaction_txn_time) as year_start,
    case when balance >= 72000 then 'LARGE' else 'SMALL' end as balance_tier,
    case when days_since_transaction_txn_time >= 77 then 'LARGE' else 'SMALL' end as days_since_transaction_txn_time_tier,
    count(*) as event_count,
    min(transaction_txn_time) as first_seen,
    min(days_since_transaction_txn_time) as min_days_since_transaction_txn_time,
    percent_rank() over (partition by date_trunc('year', transaction_txn_time) order by count(*) desc) as event_count_percent_rank
from cleaned
group by year_start, balance_tier, days_since_transaction_txn_time_tier
having min_days_since_transaction_txn_time < 1000000
order by event_count desc;
