select
    id,
    rating,
    city,
    round(rating, 1) as rating_rounded,
    upper(trim(supplier_name)) as supplier_name_normalized
from suppliers
where rating > 3.8;
