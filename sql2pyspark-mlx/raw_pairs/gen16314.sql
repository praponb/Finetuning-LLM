with base as (
    select
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        trim(plan) clean_plan
    from subscriptions
    where plan != 'basic'
),
with_windows as (
    select
        id,
        plan,
        status,
        clean_plan,
        count(monthly_fee) over w_recent running_count_monthly_fee,
        min(monthly_fee) over w_recent running_min_monthly_fee,
        max(monthly_fee) over w_recent running_max_monthly_fee,
        avg(monthly_fee) over w_recent running_avg_monthly_fee
    from base
    window w_recent as (partition by status order by monthly_fee, id rows between unbounded preceding and current row)
),
metrics as (
    select
        id,
        plan,
        status,
        clean_plan,
        running_min_monthly_fee,
        running_max_monthly_fee,
        running_avg_monthly_fee,
        avg(running_avg_monthly_fee) over w_hist running_avg_monthly_fee_avg_6_prior,
        row_number() over (partition by plan order by running_avg_monthly_fee desc, id) seq_no,
        sum(running_avg_monthly_fee) over (partition by plan, status order by running_min_monthly_fee, id rows between unbounded preceding and current row) running_running_avg_monthly_fee
    from with_windows
    window w_hist as (partition by plan order by running_max_monthly_fee, id rows between 6 preceding and 1 preceding)
),
filtered as (
    select
        id,
        plan,
        status,
        clean_plan,
        running_min_monthly_fee,
        running_avg_monthly_fee_avg_6_prior,
        seq_no,
        running_running_avg_monthly_fee,
        seq_no + running_min_monthly_fee seq_no_plus_running_min_monthly_fee,
        round(running_avg_monthly_fee * 100, 2) running_avg_monthly_fee_scaled
    from metrics
    where seq_no != 8
),
scored as (
    select
        *,
        lag(running_running_avg_monthly_fee) over (partition by plan order by running_avg_monthly_fee_avg_6_prior, id) prev_running_running_avg_monthly_fee,
        lag(seq_no) over (partition by plan order by running_avg_monthly_fee_avg_6_prior, id) prev_seq_no,
        row_number() over (partition by plan order by running_avg_monthly_fee_avg_6_prior, id) seq_no_2
    from filtered
    where seq_no_plus_running_min_monthly_fee >= 37
),
ranked as (
    select *, rank() over (partition by plan order by running_running_avg_monthly_fee) as latest_rank
    from scored
    qualify latest_rank = 1
),
ranked_subscriptions as (
    select *, row_number() over (partition by status order by seq_no_plus_running_min_monthly_fee desc, id) as dedup_rank
    from ranked
    qualify dedup_rank = 1
),
stats as (
    select
        plan,
        max(prev_seq_no) as max_prev_seq_no,
        percentile_approx(seq_no, array(0.25, 0.5, 0.75)) as seq_no_quartiles
    from ranked_subscriptions
    group by plan
)
select
    plan,
    max_prev_seq_no,
    seq_no_quartiles,
    case when plan rlike '^(enterprise|standard)$' then 'EXTENDED' when plan rlike '^(basic|premium)$' then 'PRIORITY' else 'GROUP_A' end as plan_zone
from stats
order by plan_zone nulls last
