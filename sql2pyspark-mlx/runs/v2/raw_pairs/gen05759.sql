select city, concat(country, '/', supplier_name) country_supplier_name
from suppliers
where rating <= 3.5 or supplier_name like '%Lio%'
