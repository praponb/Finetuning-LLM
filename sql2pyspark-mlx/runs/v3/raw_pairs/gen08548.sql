select case when year(o.order_date) < 2024 then 'before_2024' when year(o.order_date) = 2024 then '2024' else 'after_2024' end as order_cohort, min(s.cost) as min_cost, concat_ws(', ', sort_array(collect_set(s.carrier))) as carrier_values
from shipments s
left join orders o on s.order_id = o.id
group by case when year(o.order_date) < 2024 then 'before_2024' when year(o.order_date) = 2024 then '2024' else 'after_2024' end
order by min_cost desc, order_cohort asc
limit 1
