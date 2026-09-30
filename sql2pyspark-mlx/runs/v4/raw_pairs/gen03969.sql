select *
from (
    select
        account_type,
        customer_id,
        status,
        balance
    from accounts
) src
pivot (
    max(balance) as amount,
    count(1) as n
    for status in ('OPEN', 'FROZEN', 'CLOSED')
)
