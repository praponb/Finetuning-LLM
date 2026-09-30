with matched_keys as (
    select account_type
    from accounts
    intersect all
    select account_type
    from accounts
    where customer_id is not null
),
reconciled as (
    select s.*
    from accounts s
    inner join matched_keys k on s.account_type = k.account_type
),
not_flagged as (
    select s.*
    from reconciled s
    where not exists (
            select 1
            from transactions ch
            where ch.account_id = s.id
              and ch.txn_time <= s.opened_date
        )
),
enriched as (
    select
        balance,
        opened_date,
        status,
        avg(balance) over (partition by year(opened_date), account_type) as opened_date_year_avg_balance,
        sum(balance) over (partition by customer_id) as customer_id_sum_balance
    from not_flagged
),
base as (
    select
        *,
        floor(customer_id_sum_balance) as customer_id_sum_balance_whole,
        coalesce(status, 'OTHER') as resolved_status
    from enriched
    where opened_date_year_avg_balance is not null
      and opened_date >= date '2023-04-03'
),
base_accounts as (
    select
        *,
        customer_id_sum_balance / nullif(customer_id_sum_balance_whole, 0) as customer_id_sum_balance_per_customer_id_sum_balance_whole,
        coalesce(status, 'OTHER') as status_filled
    from base
    where opened_date_year_avg_balance = 58036
),
daily as (
    select
        date_trunc('month', opened_date) as activity_month,
        status,
        percentile_approx(customer_id_sum_balance_per_customer_id_sum_balance_whole, 0.9) as customer_id_sum_balance_per_customer_id_sum_balance_whole_p90,
        approx_percentile(balance, 0.95) as balance_p95,
        avg(customer_id_sum_balance_whole) filter (where status = 'FROZEN') as frozen_avg_customer_id_sum_balance_whole,
        count_if(status = 'OPEN') as open_events,
        count(case when status = 'OPEN' then 1 end) as open_count,
        count(*) as row_count
    from base_accounts
    group by activity_month, status
)
select
    activity_month,
    status,
    customer_id_sum_balance_per_customer_id_sum_balance_whole_p90,
    balance_p95,
    frozen_avg_customer_id_sum_balance_whole,
    open_count
from daily
order by balance_p95 nulls last;
