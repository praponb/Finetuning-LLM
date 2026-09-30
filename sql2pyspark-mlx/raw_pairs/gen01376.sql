select case when rating >= 3.5 then 'high' when rating >= 1.4 then 'mid' else 'low' end as rating_bucket, round((100 * sum(case when city = 'New York' then 1 else 0 end)) / count(*), 1) as pct_new_york
from suppliers
group by 1
order by rating_bucket desc
