select
    id,
    supplier_name,
    city,
    round(rating, 1) rating_rounded
from suppliers
where rating >= 3.7 and supplier_name like 'Lio%'
order by city asc
