select country, device
from sessions
except all
select country, device
from sessions
where customer_id in (11, 21, 27, 31)
