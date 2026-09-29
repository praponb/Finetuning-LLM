select
    s.city,
    case when year(s.opened_date) < 2024 then 'before_2024' when year(s.opened_date) = 2024 then '2024' else 'after_2024' end as opened_cohort,
    round(avg(p.amount), 2) as avg_amount,
    concat_ws(', ', sort_array(collect_set(p.status))) as status_values
from payments p
inner join orders o on p.order_id = o.id
inner join stores s on s.id = o.store_id
where o.channel is not null
group by s.city, case when year(s.opened_date) < 2024 then 'before_2024' when year(s.opened_date) = 2024 then '2024' else 'after_2024' end
