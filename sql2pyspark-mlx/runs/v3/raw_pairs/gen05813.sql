select year(launch_date) as launch_year, max(launch_date) as latest_launch_date, sort_array(collect_set(brand)) as brand_list
from products
where cost is not null and array_contains(tags, 'clearance')
group by year(launch_date)
