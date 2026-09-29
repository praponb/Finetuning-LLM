select month(opened_date) as opened_month, count(*) as num_stores, round((100 * sum(case when city = 'Berlin' then 1 else 0 end)) / count(*), 1) as pct_berlin
from stores
group by month(opened_date)
having count(*) < 4
