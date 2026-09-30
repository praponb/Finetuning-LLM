select
    year(c.signup_date) as signup_year,
    case when c.country in ('DE', 'JP') then 'group_a' else 'other' end as country_group,
    concat_ws(', ', sort_array(collect_set(s.device))) as device_values,
    count(distinct s.device) as unique_devices
from sessions s
inner join customers c on c.id = s.customer_id
where c.email is not null
group by year(c.signup_date), case when c.country in ('DE', 'JP') then 'group_a' else 'other' end
order by device_values
