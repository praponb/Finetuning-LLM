with Src as (
    select *
    from suppliers
    where country <> 'US'
),
suppliers_stats as (
    select
        case when rating >= 4.3 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket,
        city,
        max(rating) as max_rating,
        sum(case when city = 'Singapore' then rating else 0 end) as singapore_rating
    from Src
    group by case when rating >= 4.3 then 'high' when rating >= 2.0 then 'mid' else 'low' end, city
)
select rating_bucket, city, max_rating
from suppliers_stats
where singapore_rating >= 44;
