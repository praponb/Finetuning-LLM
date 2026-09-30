select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, max(balance) - min(balance) as balance_range, count(*) as num_accounts
from accounts
group by case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end
order by num_accounts desc;
