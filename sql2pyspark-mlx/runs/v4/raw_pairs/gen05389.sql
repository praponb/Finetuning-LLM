with filtered as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        case when method = 'promptpay' then 1 else 0 end as is_promptpay
    from payments
    where method = 'bank_transfer'
),
metrics as (
    -- Ranking and gap features
    select
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        is_promptpay,
        last_value(amount) ignore nulls over (partition by method order by is_promptpay, id rows between unbounded preceding and current row) as last_known_amount,
        sum(is_promptpay) over (partition by method, status order by is_promptpay, id rows between unbounded preceding and current row) as running_is_promptpay,
        sum(amount) over (partition by method, order_id order by paid_at, id rows between unbounded preceding and current row) as running_amount,
        round(amount / sum(amount) over (partition by status), 4) as amount_share
    from filtered
),
valid as (
    -- Keep recent, valid rows
    select
        *,
        substring(status, 1, 3) as status_prefix,
        coalesce(method, 'OTHER') as method_clean
    from metrics
    where paid_at >= timestamp '2024-04-25 18:00:00'
      and method = 'bank_transfer'
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        amount,
        paid_at,
        status,
        is_promptpay,
        last_known_amount,
        running_is_promptpay,
        running_amount,
        amount_share,
        status_prefix,
        method_clean,
        concat_ws('-', method, status) as method_status_key,
        coalesce(running_amount, 0) as running_amount_filled
    from valid
    where amount_share between 0.42 and 0.75
),
metrics_payments as (
    -- Running and moving aggregates
    select
        *,
        sum(running_amount) over (partition by status order by cast(paid_at as bigint) range between 7776000 preceding and current row) as running_amount_sum_90d,
        sum(amount_share) over (partition by order_id order by cast(paid_at as bigint) range between 2592000 preceding and current row) as amount_share_sum_30d,
        round(running_amount / sum(running_amount) over (partition by order_id), 4) as running_amount_share
    from cleaned
),
enriched as (
    -- Running and moving aggregates
    select
        *,
        ntile(10) over (partition by status order by running_amount_sum_90d desc, id) as running_amount_sum_90d_bucket,
        round(running_amount_sum_90d / sum(running_amount_sum_90d) over (partition by status), 4) as running_amount_sum_90d_share,
        lag(last_known_amount, 2) over (partition by order_id order by running_amount_share, id) as prev_last_known_amount
    from metrics_payments
)
-- Grouped metrics
select
    case when running_amount_filled >= 61 then 'HIGH' else 'LOW' end as running_amount_filled_tier,
    (unix_timestamp(max(paid_at)) - unix_timestamp(min(paid_at))) / 60 as duration_min,
    percentile_approx(amount_share_sum_30d, 0.9) as amount_share_sum_30d_p90,
    count(case when status = 'REFUNDED' then 1 end) as refunded_count,
    count_if(status = 'REFUNDED') as refunded_events,
    count(distinct status) as distinct_status,
    sum(case when status = 'REFUNDED' then amount_share else 0 end) as refunded_amount_share
from enriched
group by case when running_amount_filled >= 61 then 'HIGH' else 'LOW' end
having refunded_count < 1
order by refunded_events nulls last;
