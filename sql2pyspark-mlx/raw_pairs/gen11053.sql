select brand, sum(case when brand = 'Umbrella' then 1 else 0 end) as umbrella_count
from products
group by brand
having sum(case when brand = 'Umbrella' then 1 else 0 end) < 65
