select rating_bucket, country, tokyo_rating
from (
    select case when rating >= 4.2 then 'high' when rating >= 2.2 then 'mid' else 'low' end as rating_bucket, country, sum(case when city = 'Tokyo' then rating else 0 end) as tokyo_rating
    from suppliers
    where city = 'London'
    group by case when rating >= 4.2 then 'high' when rating >= 2.2 then 'mid' else 'low' end, country
) as sub
where tokyo_rating > 66
