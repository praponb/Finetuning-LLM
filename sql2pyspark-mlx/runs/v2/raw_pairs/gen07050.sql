select id, signup_date, cast(is_active as int) as is_active_flag
from customers
order by signup_date desc, id
limit 1;
