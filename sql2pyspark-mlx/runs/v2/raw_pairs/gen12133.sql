select
    a.status,
    a.account_type,
    a.customer_id,
    c.city,
    c.country
from accounts a
inner join customers c on a.customer_id = c.id
where c.country not in ('US', 'SG')
