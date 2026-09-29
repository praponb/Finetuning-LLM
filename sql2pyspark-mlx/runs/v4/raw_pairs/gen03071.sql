with cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        round(balance * 0.85, 2) as balance_scaled,
        upper(status) as status_upper,
        coalesce(status, 'OTHER') as status_filled
    from accounts
    where status is not null
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select *, round(balance_scaled, 1) as balance_scaled_rounded
    from cleaned
    where opened_date > date '2023-04-28'
      and customer_id is not null
      and balance >= 49000
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        ntile(5) over (partition by account_type, customer_id order by balance, id) as balance_bucket,
        row_number() over (partition by account_type order by opened_date desc, id) as event_seq,
        ntile(5) over (partition by customer_id order by balance_scaled_rounded desc, id) as balance_scaled_rounded_bucket,
        stddev_samp(balance) over (partition by customer_id) as customer_id_stddev_samp_balance
    from filtered
),
matched as (
    -- Non-equi join on a validity window
    select
        e.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from metrics e
    left join subscriptions sub
        on e.customer_id = sub.customer_id
       and e.opened_date between sub.start_date and sub.end_date
)
-- Grouped metrics
select
    plan,
    max(balance_scaled_rounded_bucket) as max_balance_scaled_rounded_bucket,
    approx_percentile(balance_scaled_rounded_bucket, array(0.25, 0.5, 0.75)) as balance_scaled_rounded_bucket_quartiles
from matched
group by plan
having max(balance_scaled_rounded_bucket) >= 0;
