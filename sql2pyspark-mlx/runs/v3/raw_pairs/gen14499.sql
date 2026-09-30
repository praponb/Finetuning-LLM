select
    s.city,
    s.country,
    p.cost,
    p.price,
    p.launch_date
from suppliers s
left outer join products p on s.id = p.supplier_id
where not (p.stock_qty != 144 and p.launch_date > '2025-03-01')
order by s.country desc, p.launch_date
