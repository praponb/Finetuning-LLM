select
    id,
    status,
    end_date,
    customer_id,
    date_format(end_date, 'MMM yyyy') as end_date_label
from subscriptions
where customer_id in (12, 18, 27, 37)
