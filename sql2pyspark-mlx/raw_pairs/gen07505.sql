select case when status in ('ACTIVE', 'CANCELLED') then 'core' else 'other' end as status_group, quarter(end_date) as end_quarter, min(end_date) as first_end_date
from subscriptions
where start_date <= '2023-07-06' and customer_id is not null and monthly_fee is null
group by 1, 2
order by first_end_date desc
