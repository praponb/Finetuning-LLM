select
    c.id,
    c.credit_limit,
    c.segment,
    c.age
from customers c
where exists (select 1 from accounts a where a.customer_id = c.id and a.account_type = 'SAVINGS')
