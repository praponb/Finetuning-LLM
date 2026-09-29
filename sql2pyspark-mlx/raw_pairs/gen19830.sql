with valid as (
    -- Filter the raw rows and derive helper columns
    select *, floor(monthly_fee) as monthly_fee_whole
    from subscriptions
    where customer_id is not null
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(monthly_fee, true) over (partition by plan order by monthly_fee, id rows between unbounded preceding and current row) as last_known_monthly_fee,
        ntile(4) over (order by monthly_fee_whole desc, id) as monthly_fee_whole_bucket,
        last_value(monthly_fee_whole, true) over w_ordered as last_known_monthly_fee_whole,
        row_number() over w_hist as row_seq
    from valid
    window w_ordered as (partition by status order by start_date, id rows between unbounded preceding and current row), w_hist as (partition by customer_id, status order by end_date desc, id)
),
latest as (
    -- Deduplicate with ROW_NUMBER
    select *, row_number() over (partition by status order by row_seq nulls last, id) as row_num
    from metrics
    qualify row_num = 1
),
kept_keys as (
    -- Set difference on keys
    select plan, status
    from latest
    except distinct
    select plan, status
    from subscriptions
    where customer_id is not null
),
reconciled as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from latest s
    inner join kept_keys k on s.plan = k.plan and s.status = k.status
),
with_windows as (
    -- Ranking and gap features
    select
        customer_id,
        plan,
        start_date,
        end_date,
        status,
        last_known_monthly_fee,
        last_known_monthly_fee_whole,
        row_seq,
        row_num,
        count(last_known_monthly_fee_whole) over (partition by status) as status_count_last_known_monthly_fee_whole,
        rank() over (partition by status order by row_seq desc nulls first) as row_seq_rank
    from reconciled
),
totals as (
    -- Grouped metrics
    select
        status,
        date_trunc('quarter', end_date) as quarter_start,
        approx_percentile(row_seq, array(0.25, 0.5, 0.75)) as row_seq_quartiles,
        round(avg(last_known_monthly_fee), 2) as avg_last_known_monthly_fee,
        sort_array(collect_set(plan)) as plan_list,
        size(collect_set(plan)) as num_plans,
        grouping_id() as grouping_level
    from with_windows
    group by grouping sets ((status, date_trunc('quarter', end_date)), (status), ())
)
select
    status,
    quarter_start,
    avg_last_known_monthly_fee,
    plan_list,
    num_plans
from totals;
