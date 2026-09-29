select case when s.rating >= 4.5 then 'high' when s.rating >= 1.6 then 'mid' else 'low' end as rating_bucket, count(case when p.category = 'Home' then 1 end) as home_count
from products p
left join suppliers s on s.id = p.supplier_id
where array_contains(p.tags, 'clearance')
group by case when s.rating >= 4.5 then 'high' when s.rating >= 1.6 then 'mid' else 'low' end
order by rating_bucket, home_count desc;
