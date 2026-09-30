select year(opened_date) as opened_year, max(sqft) as max_sqft
from stores
group by 1
having max(sqft) > 7000
order by opened_year, max_sqft desc
limit 50;
