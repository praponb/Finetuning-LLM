select
    s.id,
    s.country,
    s.rating,
    s.supplier_name
from suppliers s
where exists (select 1 from products p where p.supplier_id = s.id and p.launch_date < '2025-07-03');
