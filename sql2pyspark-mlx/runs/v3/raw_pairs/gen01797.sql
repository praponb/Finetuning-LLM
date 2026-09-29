select
    case when year(s.opened_date) < 2024 then 'before_2024' when year(s.opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort,
    s.city,
    count(o.shipping_fee) as shipping_fee_count,
    sort_array(collect_set(o.status)) as status_list
from orders o
right outer join customers c on o.customer_id = c.id
join stores s on o.store_id = s.id
where s.region = 'East'
group by case when year(s.opened_date) < 2024 then 'before_2024' when year(s.opened_date) = 2024 then '2024' else 'after_2024' end, s.city;
