select case when balance >= 89000 then 'high' when balance >= 24000 then 'mid' else 'low' end as balance_bucket, max(balance) as max_balance, count(*) as cnt
from accounts
group by case when balance >= 89000 then 'high' when balance >= 24000 then 'mid' else 'low' end
having cnt < 4;
