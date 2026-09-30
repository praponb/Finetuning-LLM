select
    s.id,
    s.city,
    s.supplier_name,
    s.rating
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id and p.launch_date between '2024-12-01' and '2025-01-02');
