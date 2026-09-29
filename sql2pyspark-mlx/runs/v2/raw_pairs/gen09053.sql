select
    country,
    is_active,
    count(credit_limit) as credit_limit_count,
    round(stddev(credit_limit), 2) as stddev_credit_limit
from customers
where credit_limit >= 13300
group by country, is_active
having stddev_credit_limit < 53
order by country
