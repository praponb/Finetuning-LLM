select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, sum(case when account_type = 'CHECKING' then 1 else 0 end) as checking_count
from accounts
group by opened_cohort
order by opened_cohort;
