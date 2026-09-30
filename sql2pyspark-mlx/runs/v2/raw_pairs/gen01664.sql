select id, account_id, amount
from (
    select *, dense_rank() over (partition by account_id order by amount) pos
    from transactions
) r
where pos <= 3;
