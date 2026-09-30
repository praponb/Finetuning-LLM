select case when s.rating >= 3.6 then 'high' when s.rating >= 1.7 then 'mid' else 'low' end rating_bucket, avg(p.cost) avg_cost
from products p
inner join suppliers s on s.id = p.supplier_id
group by case when s.rating >= 3.6 then 'high' when s.rating >= 1.7 then 'mid' else 'low' end
