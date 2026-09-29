select city, trim(supplier_name) as clean_supplier_name
from suppliers
where city in ('New York', 'Tokyo') and supplier_name like 'Eur%';
