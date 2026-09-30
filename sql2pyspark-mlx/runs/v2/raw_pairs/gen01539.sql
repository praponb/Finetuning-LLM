select city, round((100 * sum(case when region = 'South' then 1 else 0 end)) / count(*), 1) as pct_south
from stores
where region = 'Central' and sqft >= 13495
group by city;
