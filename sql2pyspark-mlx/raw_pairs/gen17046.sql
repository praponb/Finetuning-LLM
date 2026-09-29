select country, case when rating >= 4.5 then 'high' when rating >= 1.8 then 'mid' else 'low' end as rating_bucket, count(rating) as rating_count
from suppliers
where lower(supplier_name) like '%oky%' and (supplier_name like 'T%' or country = 'UK')
group by 1, 2
having count(rating) >= 2
order by country asc;
