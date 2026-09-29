select discount, amount, case when channel in ('store', 'partner') then 'Priority' else 'Other' end channel_group
from orders
where customer_id is not null and shipping_fee < 23 and amount >= 1160;
