with filtered_customers as (
    select *, concat(country, ', ', email) as country_email
    from customers
    where city not in ('Bangkok', 'New York', 'Chiang Mai') and signup_date > '2024-03-01'
),
segment_summary as (
    select segment, month(signup_date) as signup_month, max(age) - min(age) as age_range
    from filtered_customers
    group by segment, month(signup_date)
)
select segment, signup_month, age_range
from segment_summary
order by segment asc, signup_month asc
limit 20
