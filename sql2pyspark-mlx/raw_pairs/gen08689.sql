select date_trunc('MONTH', opened_date) as opened_month, case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, count(case when account_type = 'SAVINGS' then 1 end) as savings_count
from accounts
where customer_id in (6, 10, 21) or customer_id = 9
group by date_trunc('MONTH', opened_date), case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end
order by savings_count, opened_month, opened_cohort
limit 20
