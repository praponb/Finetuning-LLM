select region, month(opened_date) as opened_month, round(stddev(sqft), 2) as stddev_sqft
from stores
where region in ('West', 'North', 'Central')
group by region, month(opened_date)
having count(*) > 4
