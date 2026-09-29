select
    country,
    round(stddev(credit_limit), 2) as stddev_credit_limit,
    sum(age) as total_age,
    count(age) as age_count
from customers
group by country
order by stddev_credit_limit desc, country;
