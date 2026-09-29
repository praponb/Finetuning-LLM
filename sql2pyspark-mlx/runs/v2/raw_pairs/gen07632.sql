select
    a.customer_id,
    a.account_type,
    a.balance,
    c.segment
from accounts a
inner join customers c on c.id = a.customer_id
where c.city = 'New York' and c.email is not null;
