select id, monthly_fee, end_date
from subscriptions
where start_date between '2023-07-01' and '2023-07-14' or plan = 'basic';
