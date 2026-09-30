select
    id,
    country,
    rating,
    lower(country) as country_lower
from suppliers
where rating <= 1.8 and supplier_name like 'P%';
