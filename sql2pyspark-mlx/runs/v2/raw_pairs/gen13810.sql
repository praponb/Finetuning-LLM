select case when rating >= 3.8 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket, count(*) as row_count
from suppliers
group by rating_bucket
order by row_count desc
