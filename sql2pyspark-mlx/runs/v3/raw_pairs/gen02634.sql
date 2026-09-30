select
    id,
    account_type,
    balance,
    customer_id,
    substring(account_type, 1, 4) as account_type_prefix
from accounts
where status != 'OPEN' and opened_date between '2024-02-15' and '2024-04-11' and customer_id is not null
order by account_type desc
