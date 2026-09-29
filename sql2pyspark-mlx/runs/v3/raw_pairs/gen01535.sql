select txn_cohort, amount_range, merchant_list
from (
    select case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end as txn_cohort, max(amount) - min(amount) as amount_range, sort_array(collect_set(merchant)) as merchant_list
    from transactions
    group by case when year(txn_time) < 2024 then 'before_2024' when year(txn_time) = 2024 then '2024' else 'after_2024' end
) as sub
where amount_range >= 45
order by txn_cohort;
