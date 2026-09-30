select
    id,
    merchant,
    amount,
    account_id
from (
    select
        id,
        merchant,
        amount,
        account_id,
        row_number() over (partition by merchant order by amount desc, id) as rn
    from transactions
    where account_id in (22, 31, 33, 35)
) as r
where rn = 1
