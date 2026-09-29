select
    id,
    account_type,
    opened_date,
    balance,
    rank() over (partition by account_type order by opened_date desc) rnk,
    rank() over (partition by account_type order by balance desc) rnk_2
from accounts
where year(opened_date) >= 2025;
