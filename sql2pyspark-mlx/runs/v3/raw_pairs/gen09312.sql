select s.id, s.city, s.country
from suppliers s
where s.city in ('Tokyo', 'Bangkok') and s.id in (select supplier_id from products)
order by country, id
limit 5
