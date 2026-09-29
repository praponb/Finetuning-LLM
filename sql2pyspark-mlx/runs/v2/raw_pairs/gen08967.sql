select region, city, round((100 * sum(case when city = 'Singapore' then 1 else 0 end)) / count(*), 1) as pct_singapore
from stores
group by region, city;
