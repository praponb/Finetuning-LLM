select
    s.rating,
    s.supplier_name,
    s.city,
    p.stock_qty,
    p.category
from suppliers s
inner join products p on s.id = p.supplier_id
where not (p.category = 'Books' and s.rating < 2.3)
order by stock_qty asc
