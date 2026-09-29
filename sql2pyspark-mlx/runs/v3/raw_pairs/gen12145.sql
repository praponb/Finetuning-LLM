with base as (
    select *
    from accounts
    where customer_id in (2, 8, 18, 24) and (opened_date between '2024-12-01' and '2025-09-13' or customer_id in (1, 8, 25, 26))
),
status_summary as (
    select status, sum(case when status = 'OPEN' then 1 else 0 end) as open_count, sum(case when account_type = 'SAVINGS' then 1 else 0 end) as savings_count
    from base
    group by status
)
select status, open_count, savings_count
from status_summary
order by status desc
limit 10;
