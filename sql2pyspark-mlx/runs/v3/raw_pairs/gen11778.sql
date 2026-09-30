select case when city in ('London', 'Tokyo') then 'core' else 'other' end as city_group, sum(rating) as total_rating, sum(sum(rating)) over (order by case when city in ('London', 'Tokyo') then 'core' else 'other' end) as cumulative_rating
from suppliers
where rating <= 2.7
group by case when city in ('London', 'Tokyo') then 'core' else 'other' end
