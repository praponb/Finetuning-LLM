select year(signup_date) as signup_year, count(distinct age) as unique_segments, sum(case when segment = 'Corporate' then credit_limit else 0 end) as corporate_credit_limit
from customers
group by year(signup_date);
