select id, start_date, year(start_date) as start_date_year
from subscriptions
where not (customer_id in (14, 24, 34) and status not in ('CANCELLED', 'EXPIRED', 'ACTIVE'))
order by id desc, start_date_year
