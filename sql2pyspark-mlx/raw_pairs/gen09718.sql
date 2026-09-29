select
    account_type,
    status,
    case when balance >= 63000 then 'high' when balance >= 10000 then 'mid' else 'low' end as balance_bucket,
    count(distinct balance) as unique_customers,
    round(stddev(balance), 2) as stddev_balance
from accounts
where (opened_date <= '2024-08-21' and status = 'OPEN') or customer_id is null
group by account_type, status, case when balance >= 63000 then 'high' when balance >= 10000 then 'mid' else 'low' end;
