select
    case when balance >= 59000 then 'PREMIUM' else 'STANDARD' end as balance_tier,
    count_if(status = 'CLOSED') as closed_events,
    size(collect_set(status)) as num_status,
    count(case when account_type = 'CHECKING' then 1 end) as checking_count
from accounts
group by case when balance >= 59000 then 'PREMIUM' else 'STANDARD' end
