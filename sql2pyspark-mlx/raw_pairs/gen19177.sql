select suppliers.id, suppliers.country, suppliers.rating
from suppliers
where suppliers.city in ('Tokyo', 'Berlin') and not exists (select 1 from products where products.supplier_id = suppliers.id)
order by 1, suppliers.country;
