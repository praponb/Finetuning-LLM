select
    s.country,
    s.rating,
    s.city,
    p.brand,
    p.category
from suppliers s
inner join products p on s.id = p.supplier_id
