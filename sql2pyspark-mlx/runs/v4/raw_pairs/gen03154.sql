select
    id,
    customer_id,
    balance,
    opened_date
from (
    select
        id,
        customer_id,
        balance,
        opened_date,
        rank() over (partition by customer_id order by balance desc) as pos
    from accounts
    where account_type in ('LOAN', 'CREDIT', 'SAVINGS')
) as r
where pos <= 3
order by 3
