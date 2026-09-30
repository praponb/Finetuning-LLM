select s.country, p.category, p.supplier_id
from suppliers s
left join products p on p.supplier_id = s.id
where p.brand not in ('Stark', 'Initech', 'Umbrella');
