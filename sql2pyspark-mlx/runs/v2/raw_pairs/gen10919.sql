select *
from (
    select city, count(distinct rating) as unique_citys, round(stddev(rating), 2) as stddev_rating
    from suppliers
    where country = 'UK'
    group by city
) as t
where stddev_rating >= 72
order by city, unique_citys desc
limit 1
