select
    id,
    segment,
    credit_limit,
    city,
    age,
    rank() over (partition by segment order by credit_limit desc) as rnk,
    avg(age) over (partition by city) as city_avg_age
from customers
where email like '%@mail.co.th'
order by segment, 3 desc;
