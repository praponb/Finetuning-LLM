select
    id,
    country,
    age,
    rank() over (partition by country order by age) as age_rank
from customers
where segment = 'Corporate'
