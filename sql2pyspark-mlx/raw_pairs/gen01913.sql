with base as (
    -- Basic cleansing and defaults
    select
        *,
        round(datediff(end_date, start_date) / 30, 1) as months_start_date_to_end_date,
        round(datediff(end_date, start_date) / 30, 1) as months_start_date_to_end_date_2
    from subscriptions
    where customer_id is not null
      and end_date >= date '2023-05-29'
),
latest as (
    -- Keep one row per key (latest first)
    select *
    from base
    qualify row_number() over (partition by plan order by months_start_date_to_end_date desc nulls first, id) = 1
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        end_date,
        monthly_fee,
        status,
        months_start_date_to_end_date,
        avg(months_start_date_to_end_date) over (partition by month(end_date), status order by start_date, id rows between 6 preceding and current row) as months_start_date_to_end_date_avg_6,
        avg(months_start_date_to_end_date_2) over (partition by plan, customer_id order by monthly_fee, id rows between 3 preceding and current row) as months_start_date_to_end_date_2_avg_3
    from latest
),
latest_subscriptions as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by status order by end_date desc nulls first) as dedup_rank
    from enriched
    qualify dedup_rank = 1
),
base_subscriptions as (
    -- Keep recent, valid rows
    select
        *,
        monthly_fee - months_start_date_to_end_date as monthly_fee_minus_months_start_date_to_end_date,
        months_start_date_to_end_date_2_avg_3 * months_start_date_to_end_date_avg_6 as months_start_date_to_end_date_2_avg_3_times_months_start_date_to_end_date_avg_6
    from latest_subscriptions
    where end_date > to_date('2023-06-15')
),
ranked as (
    -- QUALIFY-style window filter
    select *
    from base_subscriptions
    qualify row_number() over (partition by status order by monthly_fee desc, id) = 1
),
metrics as (
    -- Window metrics over several different partitions
    select
        id,
        monthly_fee,
        status,
        months_start_date_to_end_date,
        months_start_date_to_end_date_avg_6,
        months_start_date_to_end_date_2_avg_3,
        monthly_fee_minus_months_start_date_to_end_date,
        months_start_date_to_end_date_2_avg_3_times_months_start_date_to_end_date_avg_6,
        first_value(monthly_fee) ignore nulls over w_part as first_known_monthly_fee,
        stddev_samp(months_start_date_to_end_date_2_avg_3) over (partition by status) as status_stddev_samp_months_start_date_to_end_date_2_avg_3,
        last_value(monthly_fee, true) over (order by end_date, id rows between unbounded preceding and current row) as last_known_monthly_fee
    from ranked
    where months_start_date_to_end_date_2_avg_3 = 32
    window w_part as (partition by date_trunc('week', end_date), status order by months_start_date_to_end_date_avg_6, id rows between unbounded preceding and current row)
)
select
    id,
    monthly_fee,
    months_start_date_to_end_date_avg_6,
    months_start_date_to_end_date_2_avg_3_times_months_start_date_to_end_date_avg_6
from metrics
order by id;
