select case when city in ('Chiang Mai', 'Singapore') then 'tier_1' else 'other' end as city_group, count(rating) as rating_count, min(rating) as min_rating
from suppliers
where supplier_name is not null and country <> 'UK'
group by city_group
