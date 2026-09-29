with cleaned as (
    select
        id,
        customer_id,
        balance,
        opened_date,
        status,
        datediff(current_date(), opened_date) / 7 as weeks_since_opened_date,
        coalesce(status, 'DIRECT') as status_clean
    from accounts
    where opened_date > to_date('2023-09-06')
      and status is not null
),
valid as (
    select
        id,
        customer_id,
        balance,
        status,
        weeks_since_opened_date,
        status_clean,
        balance * weeks_since_opened_date as balance_times_weeks_since_opened_date
    from cleaned
    where status = 'CLOSED'
),
metrics as (
    select
        id,
        customer_id,
        balance,
        status,
        weeks_since_opened_date,
        balance_times_weeks_since_opened_date,
        lead(balance, 1) over (partition by status order by weeks_since_opened_date, id) as next_balance,
        avg(balance_times_weeks_since_opened_date) over w_running as balance_times_weeks_since_opened_date_avg_6_prior,
        avg(weeks_since_opened_date) over (partition by customer_id order by balance_times_weeks_since_opened_date, id rows between 2 preceding and 1 preceding) as weeks_since_opened_date_avg_2_prior,
        sum(balance_times_weeks_since_opened_date) over (partition by customer_id order by balance, id rows between unbounded preceding and current row) as cumulative_balance_times_weeks_since_opened_date,
        percent_rank() over (partition by customer_id order by balance_times_weeks_since_opened_date desc nulls first) as balance_times_weeks_since_opened_date_percent_rank
    from valid
    where status = 'OPEN'
    window w_running as (partition by status order by balance_times_weeks_since_opened_date, id rows between 6 preceding and 1 preceding)
),
scored as (
    select
        *,
        avg(balance_times_weeks_since_opened_date_percent_rank) over (partition by status order by balance_times_weeks_since_opened_date_percent_rank, id rows between 6 preceding and 1 preceding) as balance_times_weeks_since_opened_date_percent_rank_avg_6_prior,
        ntile(3) over (partition by status, customer_id order by balance_times_weeks_since_opened_date_percent_rank, id) as balance_times_weeks_since_opened_date_percent_rank_bucket
    from metrics
),
joined as (
    select
        src.id,
        src.customer_id,
        src.balance_times_weeks_since_opened_date,
        src.weeks_since_opened_date_avg_2_prior,
        src.balance_times_weeks_since_opened_date_percent_rank_bucket,
        p.age
    from scored src
    left join customers p
        on src.customer_id = p.id
),
enriched as (
    select
        *,
        sum(balance_times_weeks_since_opened_date_percent_rank_bucket) over w_recent as cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket,
        row_number() over (partition by customer_id order by weeks_since_opened_date_avg_2_prior desc, id) as seq_no,
        last_value(age, true) over (partition by customer_id order by weeks_since_opened_date_avg_2_prior, id rows between unbounded preceding and current row) as last_known_age,
        row_number() over (partition by customer_id order by balance_times_weeks_since_opened_date, id) as event_seq
    from joined
    window w_recent as (partition by customer_id order by balance_times_weeks_since_opened_date_percent_rank_bucket, id rows between unbounded preceding and current row)
),
cleaned_accounts as (
    select *, case when event_seq >= 9 then 'High' when event_seq >= 4 then 'Medium' else 'Low' end as event_seq_tier
    from enriched
    where customer_id is not null
      and age != 65
)
select
    seq_no,
    case when cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket >= 45 then 'LARGE' else 'SMALL' end as cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket_tier,
    approx_percentile(last_known_age, array(0.25, 0.5, 0.75)) as last_known_age_quartiles,
    round(avg(last_known_age), 2) as avg_last_known_age,
    sum(balance_times_weeks_since_opened_date) as total_balance_times_weeks_since_opened_date,
    percentile_approx(balance_times_weeks_since_opened_date, array(0.25, 0.5, 0.75)) as balance_times_weeks_since_opened_date_quartiles,
    grouping(seq_no) as grouping_level
from cleaned_accounts
group by grouping sets ((seq_no, case when cumulative_balance_times_weeks_since_opened_date_percent_rank_bucket >= 45 then 'LARGE' else 'SMALL' end), (seq_no))
order by avg_last_known_age desc;
