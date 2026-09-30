with kept_keys as (
    -- Set difference on keys
    select method, order_id
    from payments
    except
    select method, order_id
    from payments
    where status = 'SUCCESS'
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from payments s
    inner join kept_keys k on s.method = k.method and s.order_id = k.order_id
),
scored as (
    -- Running and moving aggregates
    select
        *,
        count(amount) over (partition by order_id order by cast(paid_at as bigint) range between 7776000 preceding and current row) amount_count_90d,
        round(amount / sum(amount) over (partition by order_id), 4) amount_share
    from verified
),
valid as (
    -- Basic cleansing and defaults
    select
        id,
        method,
        amount,
        status,
        amount_count_90d,
        amount_share,
        length(method) method_length
    from scored
    where order_id is not null
),
filtered as (
    -- Basic cleansing and defaults
    select
        id,
        method,
        amount,
        amount_count_90d,
        amount_share,
        substring(status, 1, 2) status_prefix
    from valid
    where amount between 420 and 700
),
scored_payments as (
    -- Window metrics over several different partitions
    select *, stddev_samp(amount_count_90d) over (partition by method) method_stddev_samp_amount_count_90d
    from filtered
    where amount_share <= 0.29
),
valid_payments as (
    -- Filter the raw rows and derive helper columns
    select
        *,
        case when amount_share >= 0.76 then 'High' when amount_share >= 0.35 then 'Medium' else 'Low' end amount_share_level,
        least(method_stddev_samp_amount_count_90d, amount_share) least_method_stddev_samp_amount_count_90d_amount_share,
        coalesce(method, 'OTHER') method_clean
    from scored_payments
    where amount_count_90d > 14
),
filtered_payments as (
    -- Basic cleansing and defaults
    select *, round(least_method_stddev_samp_amount_count_90d_amount_share * 1.15, 2) least_method_stddev_samp_amount_count_90d_amount_share_scaled
    from valid_payments
    where method is not null
)
-- Multi-level aggregation
select
    case when amount_share >= 0.83 then 'PREMIUM' else 'STANDARD' end as amount_share_tier,
    count(*) filter (where method = 'paypal') as paypal_rows,
    sum(case when method = 'bank_transfer' then method_stddev_samp_amount_count_90d else 0 end) as bank_transfer_method_stddev_samp_amount_count_90d,
    percentile_approx(least_method_stddev_samp_amount_count_90d_amount_share_scaled, array(0.25, 0.5, 0.75)) as least_method_stddev_samp_amount_count_90d_amount_share_scaled_quartiles
from filtered_payments
group by case when amount_share >= 0.83 then 'PREMIUM' else 'STANDARD' end
order by paypal_rows;
