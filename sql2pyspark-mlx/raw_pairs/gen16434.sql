select
    supplier_name,
    rating,
    city,
    concat(city, ', ', supplier_name) as city_supplier_name
from suppliers
where city != 'London' and rating < 1.8
