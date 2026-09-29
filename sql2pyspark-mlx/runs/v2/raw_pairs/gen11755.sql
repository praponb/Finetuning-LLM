select
    id,
    rating,
    city,
    country
from suppliers
where rating > (select max(rating) from suppliers where supplier_name like 'Eu%')
