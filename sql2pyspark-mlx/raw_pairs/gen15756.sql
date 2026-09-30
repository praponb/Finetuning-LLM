select store_id, round(stddev(revenue), 2) as stddev_revenue, min(units) as min_units
from store_sales
group by store_id
having sum(revenue) < 14000
order by min_units desc, store_id
limit 10;
