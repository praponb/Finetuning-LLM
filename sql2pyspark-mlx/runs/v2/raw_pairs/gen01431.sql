select s.city, case when s.rating >= 3.4 then 'high' when s.rating >= 2.0 then 'mid' else 'low' end as rating_bucket, max(i.quantity) as max_quantity
from inventory i
join products p on i.product_id = p.id
join suppliers s on s.id = p.supplier_id
where s.rating between 1.9 and 4.0
group by s.city, case when s.rating >= 3.4 then 'high' when s.rating >= 2.0 then 'mid' else 'low' end
order by rating_bucket, city
limit 5
