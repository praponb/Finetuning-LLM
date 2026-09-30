select
    city,
    segment,
    sum(age) as total_age,
    min(age) as min_age
from customers
group by 1, 2
having min(age) > 39
order by min_age desc;
