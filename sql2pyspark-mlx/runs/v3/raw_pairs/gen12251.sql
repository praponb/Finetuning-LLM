select s.country, avg(p.price) as avg_price
from products p
inner join suppliers s on p.supplier_id = s.id
group by s.country
order by s.country, avg_price;
