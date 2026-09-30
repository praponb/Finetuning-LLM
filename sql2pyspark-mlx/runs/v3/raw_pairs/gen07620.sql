select p.category, max(i.quantity) as max_quantity
from inventory i
join products p on p.id = i.product_id
group by p.category
order by max_quantity desc, category
limit 50
