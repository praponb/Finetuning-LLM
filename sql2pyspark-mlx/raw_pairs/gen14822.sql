select status, datediff(current_date(), end_date) as days_since_end_date, datediff(current_date(), end_date) as days_since_end_date_2
from subscriptions
where monthly_fee <= 170 and (start_date > '2024-08-07' or end_date <= '2024-03-01')
order by status
