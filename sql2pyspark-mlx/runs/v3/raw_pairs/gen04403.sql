select suppliers.id, suppliers.rating, suppliers.country
from suppliers
where suppliers.id in (select supplier_id from products where product_name is not null);
