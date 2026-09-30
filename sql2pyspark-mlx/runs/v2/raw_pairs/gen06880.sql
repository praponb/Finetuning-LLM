select case when price >= 930 then 'high' when price >= 310 then 'mid' else 'low' end as price_bucket, min(cost) as min_cost, round((100 * sum(case when category = 'Books' then 1 else 0 end)) / count(*), 1) as pct_books
from products
group by 1
