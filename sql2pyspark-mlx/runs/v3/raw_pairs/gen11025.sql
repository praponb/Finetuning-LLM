select case when city in ('Tokyo', 'New York') then 'core' else 'other' end as city_group, sum(rating) as total_rating from suppliers where rating is not null group by city_group;
