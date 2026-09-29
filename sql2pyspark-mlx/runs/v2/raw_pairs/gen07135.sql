select brand, round(stddev(cost), 2) as stddev_cost
from products
where cost >= 550 and category = 'Fashion'
group by brand
order by 2 desc, brand desc;
