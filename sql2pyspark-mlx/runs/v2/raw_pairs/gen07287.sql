select *
from (
    select
        id,
        account_id,
        amount,
        row_number() over (partition by account_id order by amount, id) as pos
    from transactions
) as t
where pos <= 5;
