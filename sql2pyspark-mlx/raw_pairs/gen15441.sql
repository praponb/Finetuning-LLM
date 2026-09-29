with valid as (
    -- Basic cleansing and defaults
    select
        id,
        name,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        replace(email, ' ', '_') as email_slug
    from customers
    where age > 48
),
remaining_keys as (
    -- Set difference on keys
    select id, segment
    from valid
    except all
    select id, segment
    from customers
    where credit_limit <= 13700
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from valid s
    inner join remaining_keys k on s.id = k.id and s.segment = k.segment
),
not_flagged as (
    -- Semi-join via a correlated subquery
    select s.*
    from verified s
    where exists (
            select 1
            from orders ch
            where ch.customer_id = s.id
              and ch.order_date <= s.signup_date
        )
),
with_windows as (
    -- Running and moving aggregates
    select
        *,
        stddev_samp(credit_limit) over w_running as segment_stddev_samp_credit_limit,
        sum(credit_limit) over w_running as segment_sum_credit_limit,
        stddev_samp(credit_limit) over w_running as segment_stddev_samp_credit_limit_2
    from not_flagged
    window w_running as (partition by segment)
),
metrics as (
    -- Ranking and gap features
    select
        id,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        stddev_samp(segment_stddev_samp_credit_limit_2) over (partition by date_trunc('WEEK', signup_date), segment) as signup_date_period_stddev_samp_segment_stddev_samp_credit_limit_2,
        round(segment_stddev_samp_credit_limit_2 / sum(segment_stddev_samp_credit_limit_2) over (partition by city), 4) as segment_stddev_samp_credit_limit_2_share
    from with_windows
)
select
    id,
    is_active,
    credit_limit,
    signup_date_period_stddev_samp_segment_stddev_samp_credit_limit_2,
    segment_stddev_samp_credit_limit_2_share
from metrics
order by credit_limit desc;
