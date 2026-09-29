with cleaned as (
    select
        id,
        name,
        email,
        city,
        segment,
        signup_date,
        credit_limit,
        age,
        case when is_active then 'Yes' else 'No' end as is_active_label
    from customers
    where signup_date >= date '2023-03-14'
      and credit_limit <= 4400
),
with_windows as (
    select
        id,
        age,
        is_active_label,
        corr(credit_limit, age) over (partition by city) as corr_credit_limit_age,
        last_value(segment, true) over w_hist as last_known_segment,
        corr(credit_limit, age) over (partition by segment) as corr_credit_limit_age_2
    from cleaned
    window w_hist as (partition by city, segment order by signup_date, id rows between unbounded preceding and current row)
),
enriched as (
    select
        *,
        min(age) over (order by corr_credit_limit_age, id rows between unbounded preceding and current row) as running_min_age,
        sum(age) over (order by corr_credit_limit_age, id rows between unbounded preceding and current row) as running_sum_age,
        avg(age) over (order by corr_credit_limit_age, id rows between unbounded preceding and current row) as running_avg_age,
        count(age) over (order by corr_credit_limit_age, id rows between unbounded preceding and current row) as running_count_age
    from with_windows
    where corr_credit_limit_age_2 is null
),
with_windows_customers as (
    select
        *,
        lead(corr_credit_limit_age) over (order by running_min_age, id) as next_corr_credit_limit_age,
        lag(corr_credit_limit_age_2) over (order by running_min_age, id) as prev_corr_credit_limit_age_2,
        row_number() over (order by running_min_age, id) as seq_no
    from enriched
),
kept as (
    select s.*
    from with_windows_customers s
    where exists (
            select 1
            from orders x
            where x.customer_id = s.id
              and x.discount < 0.28
        )
),
not_flagged as (
    select s.*
    from kept s
    where not exists (
            select 1
            from subscriptions ch
            where ch.customer_id = s.id
              and ch.plan in ('premium', 'standard')
        )
)
select
    case when seq_no >= 8 then 'LARGE' else 'SMALL' end as seq_no_tier,
    case when corr_credit_limit_age_2 >= 0.37 then 'HIGH' else 'LOW' end as corr_credit_limit_age_2_tier,
    approx_percentile(running_min_age, array(0.25, 0.5, 0.75)) as running_min_age_quartiles,
    stddev_samp(age) as age_stddev,
    count(distinct id) as distinct_ids,
    round(avg(running_avg_age), 2) as avg_running_avg_age,
    percentile_approx(next_corr_credit_limit_age, array(0.25, 0.5, 0.75)) as next_corr_credit_limit_age_quartiles
from not_flagged
group by grouping sets ((case when seq_no >= 8 then 'LARGE' else 'SMALL' end, case when corr_credit_limit_age_2 >= 0.37 then 'HIGH' else 'LOW' end), (case when seq_no >= 8 then 'LARGE' else 'SMALL' end))
