select customer_id, account_type, coalesce(balance, 0) as balance_filled
from accounts
where balance <= 18000
