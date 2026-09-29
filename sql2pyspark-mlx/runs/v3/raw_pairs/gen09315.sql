select
    id,
    supplier_name,
    rating,
    country,
    floor(rating) as rating_whole,
    concat_ws('/', supplier_name, country) as supplier_name_country_key
from suppliers
where lower(trim(supplier_name)) = 'tokyo traders'
order by rating, id
limit 20
