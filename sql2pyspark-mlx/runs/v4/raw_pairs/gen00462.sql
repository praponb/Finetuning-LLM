with base as (
    -- Keep recent, valid rows
    select
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        concat_ws(',', status, plan) as status_plan_key
    from subscriptions
    where end_date > date '2024-01-25'
      and customer_id is not null
),
joined as (
    -- Attach customers attributes
    select e.*, p.name
    from base e
    left join customers p
        on e.customer_id = p.id
),
metrics as (
    -- Running and moving aggregates
    select
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        status_plan_key,
        name,
        lag(monthly_fee, 1) over (partition by status, customer_id order by monthly_fee, id) as prev_monthly_fee,
        dense_rank() over (partition by customer_id order by monthly_fee nulls first) as monthly_fee_dense_rank,
        lead(monthly_fee, 1) over w_hist as next_monthly_fee,
        dense_rank() over (partition by customer_id order by monthly_fee desc nulls last) as monthly_fee_dense_rank_2,
        avg(monthly_fee) over (partition by customer_id order by monthly_fee, id rows between 2 preceding and current row) as monthly_fee_avg_2
    from joined
    window w_hist as (partition by customer_id order by end_date, id)
),
ranked as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by customer_id order by monthly_fee_dense_rank desc nulls first, id) as rank_in_group
    from metrics
    qualify rank_in_group = 1
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        status_plan_key,
        name,
        prev_monthly_fee,
        monthly_fee_dense_rank,
        next_monthly_fee,
        monthly_fee_dense_rank_2,
        monthly_fee_avg_2,
        rank_in_group,
        row_number() over (partition by status order by prev_monthly_fee, id) as seq_no,
        lag(prev_monthly_fee, 1) over (partition by status order by monthly_fee_dense_rank_2, id) as prev_prev_monthly_fee
    from ranked
),
summary as (
    -- Grouped metrics
    select
        date_trunc('year', end_date) as year_start,
        approx_percentile(prev_prev_monthly_fee, array(0.25, 0.5, 0.75)) as prev_prev_monthly_fee_quartiles,
        size(collect_set(status)) as num_status
    from scored
    group by date_trunc('year', end_date)
    having size(collect_set(status)) < 1
)
select
    year_start,
    prev_prev_monthly_fee_quartiles,
    num_status
from summary
order by num_status;
