select case when year(a.opened_date) < 2024 then 'before_2024' when year(a.opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort, sort_array(collect_set(t.merchant)) as merchant_list
from transactions t
inner join accounts a on a.id = t.account_id
group by case when year(a.opened_date) < 2024 then 'before_2024' when year(a.opened_date) = 2024 then '2024' else 'after_2024' end;
