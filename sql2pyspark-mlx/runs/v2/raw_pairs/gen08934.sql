select region, date_format(opened_date, 'yyyy-MM') as opened_month, min(sqft) as min_sqft
from stores
group by region, date_format(opened_date, 'yyyy-MM')
having min(sqft) > 15400
order by region, min_sqft, opened_month
limit 1
