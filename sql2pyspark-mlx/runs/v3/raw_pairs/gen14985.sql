select case when balance >= 65000 then 'high' when balance >= 15000 then 'mid' else 'low' end as balance_bucket, round((100 * sum(case when status = 'OPEN' then 1 else 0 end)) / count(*), 1) as pct_open, min(opened_date) as min_opened_date
from accounts
group by balance_bucket
order by min_opened_date desc;
