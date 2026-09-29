select
    id,
    method,
    amount,
    round(amount / sum(amount) over (partition by method), 3) as amount_share
from payments;
