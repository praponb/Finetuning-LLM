with enriched as (
    -- Running and moving aggregates
    select
        *,
        count(balance) over w_part running_count_balance,
        max(balance) over w_part running_max_balance,
        avg(balance) over w_part running_avg_balance,
        min(balance) over w_part running_min_balance,
        dense_rank() over w_recent balance_dense_rank
    from accounts
    window w_part as (partition by customer_id, status order by balance, id rows between unbounded preceding and current row), w_recent as (partition by customer_id order by balance nulls last)
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from (
        select *, row_number() over (partition by account_type, status order by balance_dense_rank desc, id) as rank_in_group
        from enriched
    ) t
    where rank_in_group = 1
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from deduped s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time >= s.opened_date
        )
),
enriched_accounts as (
    -- Running and moving aggregates
    select
        *,
        lag(balance_dense_rank) over (partition by customer_id, account_type order by opened_date, id) prev_balance_dense_rank,
        lead(running_max_balance) over (partition by customer_id, account_type order by opened_date, id) next_running_max_balance,
        lag(running_max_balance) over (partition by customer_id, account_type order by opened_date, id) prev_running_max_balance,
        row_number() over (partition by customer_id, account_type order by opened_date, id) event_seq
    from kept
    where customer_id in (5, 34, 35, 38)
),
attributed as (
    -- Non-equi join on a validity window
    select
        s.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from enriched_accounts s
    inner join subscriptions sub
        on s.customer_id = sub.customer_id
       and s.opened_date >= sub.start_date
       and s.opened_date <= date_add(sub.end_date, 7)
),
latest as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by status, plan order by rank_in_group desc) as dedup_rank
    from attributed
    qualify dedup_rank = 1
),
ranked as (
    -- Deduplicate with ROW_NUMBER
    select *
    from latest
    qualify row_number() over (partition by plan order by next_running_max_balance desc, id, subscription_id) = 1
),
pivoted as (
    -- Native SQL PIVOT
    select *
    from (
        select
            status,
            plan,
            account_type,
            next_running_max_balance
        from ranked
    ) src
    pivot (
        max(next_running_max_balance) as total,
        count(1) as cnt
        for account_type in ('SAVINGS' as savings, 'LOAN' as loan)
    )
)
select
    status,
    plan,
    savings_total,
    savings_cnt,
    loan_total,
    loan_cnt,
    case when plan rlike '^(standard|enterprise)$' then 'EXTENDED' when plan rlike '^(basic|premium)$' then 'GROUP_A' else 'GROUP_B' end as plan_zone
from pivoted
order by plan desc nulls last, savings_cnt desc
