select case when balance >= 83000 then 'high' when balance >= 8000 then 'mid' else 'low' end as balance_bucket, min(opened_date) as min_opened_date, avg(balance) as avg_balance
from accounts
where not (status not in ('FROZEN', 'CLOSED') and balance between 29000 and 52000)
group by case when balance >= 83000 then 'high' when balance >= 8000 then 'mid' else 'low' end
order by avg_balance desc, balance_bucket desc;
