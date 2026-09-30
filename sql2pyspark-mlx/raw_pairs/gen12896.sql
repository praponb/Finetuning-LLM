with filtered as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        round(balance * 1.07, 2) as balance_scaled,
        initcap(trim(account_type)) as account_type_title
    from accounts
    where opened_date >= date '2023-04-01'
),
eligible as (
    -- Anti-join via a correlated subquery
    select s.*
    from filtered s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time >= '2023-11-01'
        )
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        ntile(5) over (partition by customer_id order by balance_scaled asc, id asc) as balance_scaled_bucket,
        ntile(10) over (partition by account_type order by balance asc, id asc) as balance_bucket
    from eligible
),
with_windows_accounts as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        account_type,
        balance,
        account_type_title,
        balance_scaled_bucket,
        max(balance) over (partition by customer_id order by balance_scaled_bucket asc, id asc rows between unbounded preceding and current row) as running_max_balance,
        sum(balance) over (partition by customer_id order by balance_scaled_bucket asc, id asc rows between unbounded preceding and current row) as running_sum_balance,
        avg(balance) over (partition by customer_id order by balance_scaled_bucket asc, id asc rows between unbounded preceding and current row) as running_avg_balance,
        min(balance) over (partition by customer_id order by balance_scaled_bucket asc, id asc rows between unbounded preceding and current row) as running_min_balance
    from with_windows
    where balance_scaled <= 41
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows_accounts s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.merchant in ('Amazon', 'Starbucks', '7-Eleven')
        )
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.account_type_title,
        a.balance_scaled_bucket,
        a.running_max_balance,
        a.running_avg_balance,
        a.running_min_balance,
        c.city,
        c.credit_limit,
        c.age
    from not_flagged a
    left join customers c
        on a.customer_id <=> c.id
),
with_windows_accounts_2 as (
    -- Window metrics over several different partitions
    select
        *,
        max(credit_limit) over (partition by customer_id order by running_min_balance asc, id asc rows between unbounded preceding and current row) as running_max_credit_limit,
        min(credit_limit) over (partition by customer_id order by running_min_balance asc, id asc rows between unbounded preceding and current row) as running_min_credit_limit,
        avg(credit_limit) over (partition by customer_id order by running_min_balance asc, id asc rows between unbounded preceding and current row) as running_avg_credit_limit
    from joined
)
-- Join a dimension and aggregate
select
    dim.country,
    e.city as account_city,
    count(*) as rows_total,
    min(e.running_avg_balance) as min_running_avg_balance,
    grouping_id() as level_id
from with_windows_accounts_2 e
left join customers dim on e.customer_id = dim.id
group by grouping sets ((dim.country, e.city), (dim.country))
order by account_city desc nulls first
