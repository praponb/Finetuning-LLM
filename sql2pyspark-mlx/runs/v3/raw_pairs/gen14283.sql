select balance_bucket, open_count, unique_account_types
from (
    select case when balance >= 72000 then 'high' when balance >= 30000 then 'mid' else 'low' end as balance_bucket, count(case when status = 'OPEN' then 1 end) as open_count, count(distinct balance) as unique_account_types
    from accounts
    where balance > 39000
    group by case when balance >= 72000 then 'high' when balance >= 30000 then 'mid' else 'low' end
) as s
where open_count < 6
