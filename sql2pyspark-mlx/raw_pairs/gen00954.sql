select *
from (
    select city, sum(rating) as total_rating, min(rating) as min_rating
    from suppliers
    where city is not null
    group by city
) as agg
where total_rating >= 15
order by min_rating;
