select distinct category, brand
from products
where category in ('Grocery', 'Books', 'Electronics')
order by category desc, brand
limit 50;
