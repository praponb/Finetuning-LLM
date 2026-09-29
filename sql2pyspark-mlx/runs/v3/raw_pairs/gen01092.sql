select month(opened_date) as opened_month, round(avg(sqft), 2) as avg_sqft, sort_array(collect_set(region)) as region_list
from stores
where region = 'South'
group by month(opened_date)
