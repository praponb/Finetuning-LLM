select *, date_format(start_date, 'yyyy-MM-dd') as start_date_label, date_trunc('week', start_date) as start_date_period
from subscriptions;
