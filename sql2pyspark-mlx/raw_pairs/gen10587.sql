with with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        sum(age) over (partition by month(signup_date), city) as signup_date_month_sum_age,
        round(credit_limit / sum(credit_limit) over (partition by segment, city), 4) as credit_limit_share
    from customers
),
with_windows_customers as (
    -- Window metrics over several different partitions
    select
        id,
        country,
        city,
        segment,
        signup_date_month_sum_age,
        row_number() over (partition by city order by signup_date asc, id asc) as row_seq,
        row_number() over (partition by year(signup_date), city order by credit_limit desc, id asc) as event_seq
    from with_windows
    where signup_date_month_sum_age = 195
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from with_windows_customers s
    where not exists (
            select 1
            from sessions c
            where c.customer_id = s.id
              and c.device = 'desktop'
        )
),
metrics as (
    -- Window metrics over several different partitions
    select
        *,
        avg(event_seq) over w_running as running_avg_event_seq,
        count(event_seq) over w_running as running_count_event_seq,
        min(event_seq) over w_running as running_min_event_seq,
        round(event_seq / sum(event_seq) over (partition by segment, city), 4) as event_seq_share
    from not_flagged
    window w_running as (partition by city, segment order by signup_date_month_sum_age asc, id asc rows between unbounded preceding and current row)
),
with_latest as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        top1.account_type,
        top1.balance
    from metrics s
    join lateral (
            select c.account_type, c.balance
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) top1 on true
),
valid as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        country,
        city,
        signup_date_month_sum_age,
        row_seq,
        running_avg_event_seq,
        running_count_event_seq,
        running_min_event_seq,
        event_seq_share,
        round(balance * 100, 2) as balance_adjusted
    from with_latest
    where signup_date_month_sum_age >= 117
)
-- Aggregate with conditional metrics
select
    country,
    round(sum(running_avg_event_seq) / nullif(count(case when country = 'US' then 1 end), 0), 2) as running_avg_event_seq_per_us,
    percentile_approx(event_seq_share, array(0.25, 0.5, 0.75)) as event_seq_share_quartiles,
    approx_percentile(signup_date_month_sum_age, array(0.25, 0.5, 0.75)) as signup_date_month_sum_age_quartiles,
    grouping_id() as grouping_id
from valid
group by cube (country);
