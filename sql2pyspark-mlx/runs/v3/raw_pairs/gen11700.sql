select case when c.age >= 62 then 'high' when c.age >= 35 then 'mid' else 'low' end as age_bucket, count(case when we.page = '/product' then 1 end) as product_count, count(distinct we.duration_sec) as unique_pages
from web_events we
join sessions s on s.id = we.session_id
join customers c on s.customer_id = c.id
group by case when c.age >= 62 then 'high' when c.age >= 35 then 'mid' else 'low' end
having product_count >= 3
order by unique_pages desc, age_bucket
limit 5;
