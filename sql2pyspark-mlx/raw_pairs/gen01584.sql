with filtered_accounts as (
    select *
    from accounts
    where month(opened_date) in (3, 6, 11)
),
accounts_stats as (
    select case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, case when balance >= 66000 then 'high' when balance >= 31000 then 'mid' else 'low' end as balance_bucket, min(opened_date) as first_opened_date
    from filtered_accounts
    group by case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end, case when balance >= 66000 then 'high' when balance >= 31000 then 'mid' else 'low' end
)
select opened_cohort, balance_bucket, first_opened_date
from accounts_stats
order by first_opened_date
