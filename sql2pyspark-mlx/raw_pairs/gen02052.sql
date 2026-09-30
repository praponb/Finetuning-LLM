with src as (
    select *
    from customers
    where age < 39
),
customers_stats as (
    select year(signup_date) as signup_year, sum(case when city = 'Singapore' then credit_limit else 0 end) as singapore_credit_limit, count(case when segment = 'Consumer' then 1 end) as consumer_count
    from src
    group by year(signup_date)
)
select *
from customers_stats
where consumer_count >= 9
order by signup_year desc;
