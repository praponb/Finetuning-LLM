select
    id,
    supplier_name,
    rating,
    country,
    floor(rating) rating_whole,
    round(rating, 1) rating_rounded
from suppliers
where city in ('New York', 'Tokyo')
order by supplier_name desc;
