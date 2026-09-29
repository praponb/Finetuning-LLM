select
    id,
    customer_id,
    balance,
    account_type,
    coalesce(balance, 0) balance_filled
from accounts
