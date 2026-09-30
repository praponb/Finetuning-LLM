select country, case when country in ('UK', 'US') then 'core' else 'other' end country_group, max(rating) max_rating
from suppliers
where country = 'UK' and city not in ('New York', 'London', 'Chiang Mai')
group by country, country_group
having max(rating) < 3.5
