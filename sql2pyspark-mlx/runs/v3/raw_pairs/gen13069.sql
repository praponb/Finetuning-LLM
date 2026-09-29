select
    id,
    country,
    rating,
    upper(city) city_upper
from suppliers
where (supplier_name like 'Tok%' and country = 'JP') or rating >= 4.0;
