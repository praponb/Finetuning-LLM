with enriched as (
    -- Ranking and gap features
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        cume_dist() over (partition by country order by credit_limit desc nulls last) as credit_limit_cume_dist,
        avg(age) over (partition by city order by age, id rows between 6 preceding and current row) as age_avg_6
    from customers
)
-- Aggregate with conditional metrics
select
    country,
    city,
    max(age_avg_6) as max_age_avg_6,
    sum(case when segment = 'Corporate' then credit_limit_cume_dist else 0 end) as corporate_credit_limit_cume_dist,
    approx_percentile(credit_limit_cume_dist, array(0.25, 0.5, 0.75)) as credit_limit_cume_dist_quartiles,
    percentile_approx(age, array(0.25, 0.5, 0.75)) as age_quartiles,
    round(avg(age), 2) as avg_age
from enriched
group by country, city
order by corporate_credit_limit_cume_dist desc;
