select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, account_type, count(*) as cnt
from accounts
where account_type = 'SAVINGS'
group by 1, 2
