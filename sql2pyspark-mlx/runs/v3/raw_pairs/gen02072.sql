select case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end as txn_cohort, count(case when txn_type = 'CREDIT' then 1 end) as credit_count, avg(amount) as avg_amount
from transactions
group by txn_cohort
order by avg_amount
