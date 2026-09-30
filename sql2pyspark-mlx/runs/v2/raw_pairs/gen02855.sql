select city, max(rating) as max_rating, round((100 * sum(case when city = 'Chiang Mai' then 1 else 0 end)) / count(*), 1) as pct_chiang_mai
from suppliers
group by city;
