select amount, status, coalesce(amount, 0) as amount_filled
from orders
where channel in ('web', 'partner');
