select
    id,
    txn_type,
    amount,
    account_id
from (
    select
        id,
        txn_type,
        amount,
        account_id,
        row_number() over (partition by txn_type order by amount desc, id) as pos
    from transactions
) as ranked
where pos <= 3
