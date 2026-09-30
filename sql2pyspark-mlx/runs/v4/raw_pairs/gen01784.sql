select
    case when balance >= 84000 then 'PREMIUM' else 'STANDARD' end as balance_tier,
    round(sum(balance) / nullif(count(case when account_type = 'CHECKING' then 1 end), 0), 2) as balance_per_checking,
    count(*) as num_rows,
    avg(balance) as avg_balance
from accounts
group by case when balance >= 84000 then 'PREMIUM' else 'STANDARD' end
