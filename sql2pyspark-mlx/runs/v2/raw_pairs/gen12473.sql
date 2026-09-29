select case when s.rating >= 3.5 then 'high' when s.rating >= 2.1 then 'mid' else 'low' end as rating_bucket, round(avg(p.cost), 2) as avg_cost
from products p
left join suppliers s on p.supplier_id = s.id
where s.rating < 3.5
group by case when s.rating >= 3.5 then 'high' when s.rating >= 2.1 then 'mid' else 'low' end
order by avg_cost, rating_bucket
limit 3
