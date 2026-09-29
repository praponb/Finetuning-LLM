select
    a.id,
    a.balance,
    a.status,
    c.age,
    c.is_active,
    concat_ws('-', a.status, a.account_type) status_account_type_key
from accounts a
inner join customers c on a.customer_id = c.id
