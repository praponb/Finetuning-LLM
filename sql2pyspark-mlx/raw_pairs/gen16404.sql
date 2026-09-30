select country, rating_bucket, total_rating
from (
    select country, case when rating >= 3.7 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket, sum(rating) as total_rating
    from suppliers
    where supplier_name not like '%Globa%'
    group by country, case when rating >= 3.7 then 'high' when rating >= 2.0 then 'mid' else 'low' end
) as agg
where total_rating >= 24
order by total_rating desc, country;
