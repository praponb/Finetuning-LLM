select case when s.rating >= 4.5 then 'high' when s.rating >= 2.5 then 'mid' else 'low' end as rating_bucket, s.country, count(i.quantity) as quantity_count
from inventory i
inner join products p on p.id = i.product_id
left outer join suppliers s on p.supplier_id = s.id
where s.city != 'Berlin'
group by case when s.rating >= 4.5 then 'high' when s.rating >= 2.5 then 'mid' else 'low' end, s.country
order by country desc, rating_bucket
limit 20;
