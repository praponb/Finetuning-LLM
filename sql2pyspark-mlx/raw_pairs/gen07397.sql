with base as (
    select *
    from customers
    where name like 'Pi%' and email is not null
),
segment_summary as (
    select segment, avg(age) as avg_age, round(stddev(credit_limit), 2) as stddev_credit_limit
    from base
    group by segment
)
select segment, avg_age, stddev_credit_limit
from segment_summary
order by avg_age;
