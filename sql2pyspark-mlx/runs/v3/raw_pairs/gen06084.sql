select
    id,
    customer_id,
    end_date,
    row_number() over (partition by customer_id order by end_date, id) as row_num
from subscriptions
order by row_num desc, customer_id desc;
