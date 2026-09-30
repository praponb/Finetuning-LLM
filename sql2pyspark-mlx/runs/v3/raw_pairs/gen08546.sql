select launch_date, brand, array_contains(tags, 'sale') as is_sale
from products
order by brand desc
