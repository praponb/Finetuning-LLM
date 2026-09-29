select is_active, count(distinct credit_limit) as unique_segments, count(age) as age_count
from customers
group by is_active;
