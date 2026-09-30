select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, status, max(balance) as max_balance
from accounts
where status = 'CLOSED'
group by opened_cohort, status
order by max_balance desc;
