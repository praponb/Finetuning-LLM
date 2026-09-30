with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when status in ('OPEN', 'FROZEN') then 'Tier 1' else 'Other' end status_group,
        case when status in ('CLOSED', 'OPEN') then 'Priority' else 'Other' end status_group_2,
        coalesce(status, 'DIRECT') status_clean
    from accounts
    where customer_id is not null
      and status is distinct from 'FROZEN'
),
kept_keys as (
    -- Set intersection on keys
    select account_type, status
    from cleaned
    intersect all
    select account_type, status
    from accounts
    where account_type = 'LOAN'
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from cleaned s
    inner join kept_keys k on s.account_type = k.account_type and s.status = k.status
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        avg(balance) over (partition by date_trunc('month', opened_date), account_type) opened_date_period_avg_balance,
        min(balance) over w_ordered opened_date_month_min_balance
    from reconciled
    where balance <= 77000
    window w_ordered as (partition by month(opened_date))
),
kept as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows s
    where not exists (
            select 1
            from transactions c
            where c.account_id = s.id
              and c.txn_time <= s.opened_date
        )
),
joined as (
    -- Attach customers attributes
    select
        a.customer_id,
        a.balance,
        a.opened_date,
        a.status_group,
        a.status_clean,
        a.opened_date_period_avg_balance,
        d.credit_limit,
        d.signup_date
    from kept a
    left join customers d
        on a.customer_id = d.id
),
enriched as (
    -- Window metrics over several different partitions
    select
        customer_id,
        opened_date,
        credit_limit,
        signup_date,
        max(balance) over (partition by year(opened_date), customer_id) opened_date_year_max_balance,
        avg(opened_date_period_avg_balance) over (partition by customer_id) customer_id_avg_opened_date_period_avg_balance,
        stddev_samp(credit_limit) over (partition by customer_id) customer_id_stddev_samp_credit_limit
    from joined
)
select
    opened_date,
    credit_limit,
    signup_date,
    opened_date_year_max_balance,
    customer_id_avg_opened_date_period_avg_balance,
    customer_id_stddev_samp_credit_limit
from enriched;
