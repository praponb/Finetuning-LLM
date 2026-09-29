select
    id,
    country,
    age,
    signup_date,
    credit_limit,
    round(age / sum(age) over (partition by country), 3) age_share,
    min(credit_limit) over (partition by year(signup_date)) signup_date_year_min_credit_limit
from customers;
