select id, category
from products
where upper(trim(product_name)) = 'DESK LAMP'
order by category desc
