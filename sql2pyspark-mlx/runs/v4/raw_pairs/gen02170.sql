select orders.amount, stores.city
from orders
full outer join stores on orders.store_id = stores.id
where stores.opened_date between '2024-01-22' and '2024-12-01';
