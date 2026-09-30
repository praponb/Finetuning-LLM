select
    city,
    date_trunc('quarter', signup_date) as quarter_start,
    sum(case when segment = 'Consumer' then age else 0 end) as consumer_age,
    max(age) as max_age,
    count(case when city = 'Singapore' then 1 end) as singapore_count,
    count(*) as cnt,
    round(sum(age) / nullif(count(case when city = 'Bangkok' then 1 end), 0), 2) as age_per_bangkok,
    dense_rank() over (partition by city order by count(case when city = 'Singapore' then 1 end) desc) as singapore_count_dense_rank
from customers
group by city, quarter_start
having max(age) < 0;
