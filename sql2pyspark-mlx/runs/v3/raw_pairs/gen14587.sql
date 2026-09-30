select quarter(stores.opened_date) as opened_quarter, avg(inventory.quantity) as avg_quantity
from inventory
inner join stores on inventory.store_id = stores.id
group by quarter(stores.opened_date)
order by avg_quantity desc
