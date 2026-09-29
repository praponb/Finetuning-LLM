select case when rating >= 3.8 then 'high' when rating >= 2.0 then 'mid' else 'low' end as rating_bucket, round((100 * sum(case when country = 'TH' then 1 else 0 end)) / count(*), 1) as pct_th
from suppliers
group by case when rating >= 3.8 then 'high' when rating >= 2.0 then 'mid' else 'low' end
order by pct_th desc
