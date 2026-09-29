with valid as (
    -- Basic cleansing and defaults
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        datediff(current_date(), signup_date) as days_since_signup_date,
        coalesce(city, 'OTHER') as resolved_city
    from customers
    where signup_date > date '2023-02-06'
      and city is not null
),
valid_customers as (
    -- Basic cleansing and defaults
    select
        *,
        date_trunc('week', signup_date) as signup_date_period,
        case when segment in ('SMB', 'Corporate') then 'Priority' else 'Other' end as segment_group
    from valid
    where city is not null
      and segment in ('Corporate', 'Consumer', 'SMB')
),
with_last as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.email,
        s.city,
        s.segment,
        s.credit_limit,
        s.signup_date_period,
        top1.account_type,
        top1.opened_date
    from valid_customers s
    left join lateral (
            select c.account_type, c.opened_date
            from accounts c
            where c.customer_id = s.id
            order by c.opened_date desc, c.id desc
            limit 1
        ) top1 on true
),
metrics as (
    -- Ranking and gap features
    select
        *,
        ntile(10) over (partition by segment order by credit_limit, id) as credit_limit_bucket,
        first_value(city, true) over w_recent as first_known_city,
        lag(credit_limit, 1) over (order by opened_date, id) as prev_credit_limit,
        percent_rank() over (partition by city, segment order by credit_limit nulls first) as credit_limit_percent_rank,
        stddev_samp(credit_limit) over w_ordered as account_type_stddev_samp_credit_limit
    from with_last
    window w_recent as (partition by account_type order by signup_date_period, id rows between unbounded preceding and current row), w_ordered as (partition by account_type)
),
deduped as (
    -- Deduplicate with ROW_NUMBER
    select *
    from metrics
    qualify row_number() over (partition by segment order by credit_limit nulls first, id) = 1
),
latest_match as (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.status,
        x.account_account_type
    from deduped s
    left join lateral (
            select c.status, c.account_type as account_account_type
            from accounts c
            where c.customer_id = s.id
            order by c.balance desc, c.id desc
            limit 1
        ) x on true
)
-- Running and moving aggregates
select
    id,
    segment,
    signup_date_period,
    credit_limit_bucket,
    prev_credit_limit,
    account_type_stddev_samp_credit_limit,
    status,
    row_number() over (partition by segment order by credit_limit_bucket, id) as seq_no,
    sum(account_type_stddev_samp_credit_limit) over w_grp as cumulative_account_type_stddev_samp_credit_limit,
    avg(credit_limit_percent_rank) over (partition by segment) as segment_avg_credit_limit_percent_rank,
    last_value(account_account_type, true) over w_ordered as last_known_account_account_type
from latest_match
where account_type = 'SAVINGS'
window w_grp as (partition by account_type order by credit_limit_bucket, id rows between unbounded preceding and current row), w_ordered as (partition by status order by prev_credit_limit, id rows between unbounded preceding and current row);
