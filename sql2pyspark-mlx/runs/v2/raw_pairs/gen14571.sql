select year(stores.opened_date) as opened_year, min(inventory.quantity) as min_quantity, sum(inventory.quantity) as total_quantity
from inventory
right outer join stores on inventory.store_id = stores.id
left outer join products on products.id = inventory.product_id
group by year(stores.opened_date)
order by total_quantity asc, opened_year asc
limit 50
