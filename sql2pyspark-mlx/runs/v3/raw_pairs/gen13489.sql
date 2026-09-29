select customer_id, add_months(end_date, 1) as end_date_plus_months, datediff(current_date(), start_date) as days_since_start_date
from subscriptions
where end_date between '2023-07-18' and '2024-07-01' or monthly_fee is not null;
