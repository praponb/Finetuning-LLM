select
    id,
    account_type,
    customer_id,
    status,
    coalesce(balance, 0) as balance_filled
from accounts
where opened_date > '2025-07-05'
order by balance_filled desc;
