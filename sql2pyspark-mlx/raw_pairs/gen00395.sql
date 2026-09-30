with with_latest as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.is_active,
        s.age,
        top1.device
    from customers s
    join lateral (
            select c.device
            from sessions c
            where c.customer_id = s.id
              and c.started_at <= s.signup_date
            order by c.started_at desc, c.id desc
            limit 1
        ) top1 on true
),
with_windows as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(device, true) over w_ordered as last_known_device,
        avg(age) over w_running as age_avg_3_prior
    from with_latest
    window w_ordered as (partition by device order by age, id rows between unbounded preceding and current row), w_running as (order by age, id rows between 3 preceding and 1 preceding)
),
enriched as (
    -- Window metrics over several different partitions
    select
        id,
        email,
        is_active,
        age,
        device,
        age_avg_3_prior,
        count(age_avg_3_prior) over (partition by device order by age, id rows between unbounded preceding and current row) as running_count_age_avg_3_prior,
        avg(age_avg_3_prior) over (partition by device order by age, id rows between unbounded preceding and current row) as running_avg_age_avg_3_prior,
        max(age_avg_3_prior) over (partition by device order by age, id rows between unbounded preceding and current row) as running_max_age_avg_3_prior
    from with_windows
),
enriched_customers as (
    -- Running and moving aggregates
    select
        *,
        round(age / sum(age) over (partition by device), 4) as age_share,
        lead(running_avg_age_avg_3_prior, 1) over w_hist as next_running_avg_age_avg_3_prior,
        row_number() over w_hist as event_seq,
        first_value(device, true) over (partition by device order by age_avg_3_prior, id rows between unbounded preceding and current row) as first_known_device
    from enriched
    where lower(email) like '%ack%'
    window w_hist as (partition by device order by running_max_age_avg_3_prior, id)
)
-- Multi-level aggregation
select
    device,
    count(*) as row_count,
    avg(age_avg_3_prior) filter (where device = 'tablet') as tablet_avg_age_avg_3_prior,
    approx_percentile(next_running_avg_age_avg_3_prior, array(0.25, 0.5, 0.75)) as next_running_avg_age_avg_3_prior_quartiles
from enriched_customers
group by device
having count(*) > 1;
