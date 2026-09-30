select
    signup_month,
    segment,
    stddev_age,
    customer_count
from (
    select
        date_format(signup_date, 'yyyy-MM') as signup_month,
        segment,
        round(stddev(age), 2) as stddev_age,
        count(*) as customer_count
    from customers
    where month(signup_date) in (2, 8, 12)
    group by date_format(signup_date, 'yyyy-MM'), segment
) as agg
where customer_count > 6
order by segment, signup_month
limit 10
