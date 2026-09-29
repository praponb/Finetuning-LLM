select
    id,
    country,
    age,
    avg(age) over (partition by country) as country_avg_age
from customers
where upper(email) like '%ENR%'
