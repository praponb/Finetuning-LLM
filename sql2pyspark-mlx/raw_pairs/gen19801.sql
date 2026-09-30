select suppliers.country, suppliers.city, products.launch_date
from suppliers
left join products on suppliers.id = products.supplier_id;
