select distinct brand, category
from products
where category = 'Fashion'
order by category, brand
limit 5;
