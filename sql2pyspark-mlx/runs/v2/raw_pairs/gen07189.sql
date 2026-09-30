select status, end_date
from subscriptions
where month(start_date) in (3, 5, 7) and end_date > '2024-09-01'
order by end_date desc
