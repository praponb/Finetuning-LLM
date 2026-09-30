select brand, sum(price) as total_price, sum(sum(price)) over (order by brand) as cumulative_price
from products
group by brand
order by brand desc;
