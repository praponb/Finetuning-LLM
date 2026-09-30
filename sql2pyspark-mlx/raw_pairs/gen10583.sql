select
    id,
    customer_id,
    account_type,
    balance
from accounts
where status = 'FROZEN'
order by id desc, balance desc
limit 5;
