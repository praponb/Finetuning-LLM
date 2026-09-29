select id, start_date
from subscriptions
where start_date = (select max(start_date) from subscriptions);
