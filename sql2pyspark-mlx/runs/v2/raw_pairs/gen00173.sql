select
    id,
    customer_id,
    opened_date,
    balance,
    account_type,
    lead(balance, 2, 0) over (partition by customer_id order by opened_date, id) as next_balance,
    percent_rank() over (partition by account_type order by opened_date) as opened_date_pct_rank
from accounts
where account_type = 'LOAN';
