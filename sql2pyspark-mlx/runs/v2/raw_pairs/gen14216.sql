select
    id,
    account_id,
    amount,
    dense_rank() over (partition by account_id order by amount) as dense_rnk
from transactions
order by account_id
