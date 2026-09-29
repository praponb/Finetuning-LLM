select account_type, customer_id
from accounts
where account_type != 'SAVINGS'
intersect
select account_type, customer_id
from accounts
where account_type in ('CHECKING', 'CREDIT')
order by customer_id;
