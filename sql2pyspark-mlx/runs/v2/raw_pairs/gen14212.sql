select * from (select case when rating >= 3.6 then 'high' when rating >= 2.2 then 'mid' else 'low' end as rating_bucket, city, rating from suppliers)
pivot (sum(rating) for city in ('Berlin', 'London'))
order by rating_bucket
