with cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        name,
        country,
        segment,
        is_active,
        credit_limit,
        round(age * 1.15, 2) as age_scaled,
        concat_ws('/', country, email) as country_email_key
    from customers
    where signup_date >= to_date('2023-02-15')
),
with_last as (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.segment,
        s.is_active,
        s.credit_limit,
        s.country_email_key,
        latest.session_device
    from cleaned s
    left join lateral (
            select c.device as session_device
            from sessions c
            where c.customer_id = s.id
            order by c.started_at desc, c.id desc
            limit 1
        ) latest on true
),
metrics as (
    -- Ranking and gap features
    select
        *,
        min(credit_limit) over (partition by segment order by credit_limit, id rows between 3 preceding and current row) as last4_min_credit_limit,
        sum(credit_limit) over (partition by segment order by credit_limit, id rows between 3 preceding and current row) as last4_sum_credit_limit,
        avg(credit_limit) over (partition by segment order by credit_limit, id rows between 3 preceding and current row) as last4_avg_credit_limit
    from with_last
),
metrics_customers as (
    -- Running and moving aggregates
    select
        id,
        segment,
        credit_limit,
        session_device,
        last4_min_credit_limit,
        last4_sum_credit_limit,
        last4_avg_credit_limit,
        ntile(3) over (partition by session_device order by last4_min_credit_limit desc, id) as last4_min_credit_limit_bucket,
        avg(last4_avg_credit_limit) over (partition by segment order by credit_limit, id rows between 2 preceding and 1 preceding) as last4_avg_credit_limit_avg_2_prior
    from metrics
),
daily as (
    -- Aggregate with conditional metrics
    select
        segment,
        round(avg(last4_sum_credit_limit), 2) as avg_last4_sum_credit_limit,
        sort_array(collect_set(session_device)) as session_device_list,
        approx_percentile(last4_min_credit_limit, 0.9) as last4_min_credit_limit_p90,
        round(avg(last4_avg_credit_limit), 2) as avg_last4_avg_credit_limit,
        min(credit_limit) as min_credit_limit,
        count_if(segment = 'SMB') as smb_events
    from metrics_customers
    group by segment
)
select
    segment,
    avg_last4_sum_credit_limit,
    session_device_list,
    avg_last4_avg_credit_limit,
    min_credit_limit,
    smb_events
from daily
order by avg_last4_avg_credit_limit desc;
