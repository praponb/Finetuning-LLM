select rating_bucket, avg_rating
from (
    select case when rating >= 4.5 then 'high' when rating >= 1.6 then 'mid' else 'low' end as rating_bucket, round(avg(rating), 2) as avg_rating
    from suppliers
    group by case when rating >= 4.5 then 'high' when rating >= 1.6 then 'mid' else 'low' end
) as x
where avg_rating < 37
order by 1 desc, avg_rating desc;
