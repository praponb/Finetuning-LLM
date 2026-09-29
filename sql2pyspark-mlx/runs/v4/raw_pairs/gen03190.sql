with base as (
    -- Keep recent, valid rows
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        ceil(age) as age_whole,
        coalesce(segment, 'UNKNOWN') as resolved_segment
    from customers
    where is_active = true
),
metrics as (
    -- Ranking and gap features
    select
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age_whole,
        resolved_segment,
        ntile(5) over (partition by segment order by age_whole desc, id) as age_whole_bucket,
        avg(credit_limit) over (partition by city, country order by credit_limit, id rows between 5 preceding and 1 preceding) as credit_limit_avg_5_prior,
        last_value(credit_limit) ignore nulls over (partition by city order by credit_limit, id rows between unbounded preceding and current row) as last_known_credit_limit
    from base
),
matched_keys as (
    -- Set intersection on keys
    select country
    from metrics
    intersect all
    select country
    from customers
    where age = 38
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from metrics s
    inner join matched_keys k on s.country = k.country
),
enriched as (
    -- Window metrics over several different partitions
    select
        *,
        percent_rank() over (partition by country, city order by last_known_credit_limit desc) as last_known_credit_limit_percent_rank,
        round(credit_limit_avg_5_prior / sum(credit_limit_avg_5_prior) over (partition by city, country), 4) as credit_limit_avg_5_prior_share,
        dense_rank() over (partition by country order by credit_limit_avg_5_prior nulls last) as credit_limit_avg_5_prior_dense_rank,
        round(credit_limit / sum(credit_limit) over (partition by city), 4) as credit_limit_share,
        dense_rank() over (order by last_known_credit_limit desc nulls first) as last_known_credit_limit_dense_rank
    from verified
),
kept_keys as (
    -- Set intersection on keys
    select segment, city
    from enriched
    intersect
    select segment, city
    from customers
    where age < 64
),
kept_rows as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    inner join kept_keys k on s.segment = k.segment and s.city = k.city
),
metrics_customers as (
    -- Ranking and gap features
    select
        *,
        dense_rank() over (partition by segment order by credit_limit_share) as credit_limit_share_dense_rank,
        round(credit_limit / sum(credit_limit) over (partition by city), 4) as credit_limit_share_2,
        sum(credit_limit_share) over (partition by country, segment) as country_sum_credit_limit_share,
        min(age_whole) over (partition by city) as city_min_age_whole
    from kept_rows
),
base_customers as (
    -- Keep recent, valid rows
    select *, round(city_min_age_whole, 1) as city_min_age_whole_rounded
    from metrics_customers
    where signup_date >= date '2023-07-30'
      and segment is not null
      and last_known_credit_limit_percent_rank between 0.48 and 0.49
)
select
    id,
    country,
    segment,
    signup_date,
    credit_limit,
    age_whole_bucket,
    last_known_credit_limit,
    last_known_credit_limit_percent_rank,
    credit_limit_avg_5_prior_share
from base_customers;
