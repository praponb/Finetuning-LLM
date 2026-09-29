select
    a.account_type,
    case when year(a.opened_date) < 2024 then 'before_2024' when year(a.opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort,
    sum(case when t.merchant = 'Amazon' then 1 else 0 end) as amazon_count,
    round((100 * sum(case when t.merchant = 'Shell' then 1 else 0 end)) / count(*), 1) as pct_shell
from transactions t
join accounts a on t.account_id = a.id
group by a.account_type, case when year(a.opened_date) < 2024 then 'before_2024' when year(a.opened_date) = 2024 then '2024' else 'after_2024' end
