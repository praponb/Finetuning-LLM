select s.city, round(stddev(ss.units), 2) as stddev_units, sum(ss.units) as total_units
from store_sales ss
right outer join stores s on ss.store_id = s.id
group by s.city
having total_units > 170
order by s.city desc
