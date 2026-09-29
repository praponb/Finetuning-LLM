select s.id, s.supplier_name, s.rating
from suppliers s
where s.supplier_name like 'Tok%' and not exists (select 1 from products p where p.supplier_id = s.id and p.stock_qty >= 267)
order by rating desc, id
limit 10
