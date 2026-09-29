select case when s.rating >= 3.5 then 'high' when s.rating >= 2.2 then 'mid' else 'low' end rating_bucket, count(distinct p.price) unique_suppliers
from products p
left outer join suppliers s on p.supplier_id = s.id
where s.city != 'Chiang Mai'
group by case when s.rating >= 3.5 then 'high' when s.rating >= 2.2 then 'mid' else 'low' end
having unique_suppliers > 4
order by rating_bucket
limit 10
