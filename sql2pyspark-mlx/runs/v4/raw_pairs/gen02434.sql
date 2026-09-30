with valid as (
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        length(city) city_length,
        initcap(trim(city)) city_title
    from customers
    where signup_date > to_date('2023-07-11')
      and segment is not null
),
scored as (
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        city_length,
        city_title,
        lead(credit_limit, 1) over (order by signup_date, id) next_credit_limit,
        first_value(credit_limit) ignore nulls over (partition by segment order by city_length, id rows between unbounded preceding and current row) first_known_credit_limit
    from valid
),
kept as (
    select s.*
    from scored s
    where exists (
            select 1
            from subscriptions x
            where x.customer_id = s.id
              and x.monthly_fee between 60 and 110
        )
),
grouped as (
    select
        city,
        count(*) as num_rows,
        count(distinct id) as distinct_ids,
        approx_percentile(next_credit_limit, 0.95) as next_credit_limit_p95,
        percentile_approx(city_length, array(0.25, 0.5, 0.75)) as city_length_quartiles,
        sum(case when segment = 'Consumer' then next_credit_limit else 0 end) as consumer_next_credit_limit,
        count(*) as row_count
    from kept
    group by city
)
select
    city,
    distinct_ids,
    next_credit_limit_p95,
    city_length_quartiles,
    consumer_next_credit_limit,
    row_count
from grouped
order by consumer_next_credit_limit nulls last, distinct_ids;
