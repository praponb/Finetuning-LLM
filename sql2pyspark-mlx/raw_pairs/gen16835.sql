with deduped as (
    select *, row_number() over (partition by city order by credit_limit, id) as row_num
    from customers
    qualify row_num = 1
),
latest as (
    select *
    from deduped
    qualify row_number() over (partition by country order by signup_date nulls first, id) = 1
),
metrics as (
    select
        *,
        avg(age) over (partition by month(signup_date), segment order by signup_date, id rows between 2 preceding and 1 preceding) as age_avg_2_prior,
        round(age / sum(age) over (partition by city, country), 4) as age_share,
        avg(age) over w_ordered as age_avg_3_prior,
        count(credit_limit) over (partition by year(signup_date), country) as signup_date_year_count_credit_limit
    from latest
    where country in ('DE', 'JP')
    window w_ordered as (partition by country, city order by row_num, id rows between 3 preceding and 1 preceding)
),
deduped_customers as (
    select *
    from (
        select *, row_number() over (partition by country order by credit_limit desc, id) as row_num_2
        from metrics
    ) ranked
    where row_num_2 = 1
),
enriched as (
    select
        id,
        name,
        signup_date,
        is_active,
        credit_limit,
        row_num,
        age_avg_2_prior,
        age_avg_3_prior,
        ntile(5) over w_grp as age_bucket,
        sum(age_avg_2_prior) over w_hist as cumulative_age_avg_2_prior,
        lead(signup_date_year_count_credit_limit, 2) over (order by signup_date, id) as next_signup_date_year_count_credit_limit
    from deduped_customers
    window w_grp as (order by age desc, id), w_hist as (partition by segment, city order by credit_limit, id rows between unbounded preceding and current row)
),
scored as (
    select
        *,
        last_value(next_signup_date_year_count_credit_limit) ignore nulls over (order by credit_limit, id rows between unbounded preceding and current row) as last_known_next_signup_date_year_count_credit_limit,
        sum(age_avg_2_prior) over (order by next_signup_date_year_count_credit_limit, id rows between unbounded preceding and current row) as cumulative_age_avg_2_prior_2,
        ntile(3) over (order by credit_limit, id) as credit_limit_bucket,
        row_number() over (order by next_signup_date_year_count_credit_limit, id) as event_seq,
        avg(credit_limit) over (partition by date_trunc('month', signup_date)) as signup_date_period_avg_credit_limit
    from enriched
)
select
    id,
    name,
    signup_date,
    is_active,
    credit_limit,
    age_bucket,
    cumulative_age_avg_2_prior,
    next_signup_date_year_count_credit_limit,
    cumulative_age_avg_2_prior_2
from scored
order by cumulative_age_avg_2_prior desc nulls first;
