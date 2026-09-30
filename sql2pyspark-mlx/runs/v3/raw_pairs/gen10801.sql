select
    status,
    case when year(opened_date) < 2024 then 'before_2024' when year(opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort,
    date_format(opened_date, 'yyyy-MM') as opened_month,
    max(balance) - min(balance) as balance_range
from accounts
group by 1, 2, 3
order by opened_cohort;
