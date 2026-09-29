select status
from orders
where channel = 'web'
union
select status
from payments;
