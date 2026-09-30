select region, min(opened_date) first_opened_date
from stores
where region is null and opened_date > '2024-11-27' and store_name like '%Siam%'
group by region
having count(*) >= 2
order by region
