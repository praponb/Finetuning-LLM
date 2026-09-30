select
    case when balance >= 80000 then 'high' when balance >= 31000 then 'mid' else 'low' end as balance_bucket,
    account_type,
    avg(balance) as avg_balance,
    concat_ws(', ', sort_array(collect_set(account_type))) as account_type_values
from accounts
where (opened_date between '2024-03-01' and '2025-10-14' and customer_id in (8, 12, 23, 32)) or account_type in ('SAVINGS', 'CREDIT')
group by 1, 2
order by avg_balance desc;
