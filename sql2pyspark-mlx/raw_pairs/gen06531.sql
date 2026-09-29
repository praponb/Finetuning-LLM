select case when year(paid_at) < 2024 then 'before_2024' when year(paid_at) = 2024 then '2024' else 'after_2024' end paid_cohort, count(*) row_count from payments group by 1
