select country, sum(rating) as total_rating
from suppliers
where country in ('SG', 'US') and supplier_name is not null
group by country
having min(rating) > 3.8
order by total_rating desc;
