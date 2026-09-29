select case when s.rating >= 3.6 then 'high' when s.rating >= 2.6 then 'mid' else 'low' end as rating_bucket, avg(i.quantity) as avg_quantity
from inventory i
join products p on i.product_id = p.id
join suppliers s on p.supplier_id = s.id
where p.brand in ('Acme', 'Umbrella', 'Initech')
group by case when s.rating >= 3.6 then 'high' when s.rating >= 2.6 then 'mid' else 'low' end
having avg(i.quantity) >= 200
order by rating_bucket
limit 1;
