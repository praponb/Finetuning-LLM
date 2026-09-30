select
    year(c.signup_date) as signup_year,
    c.is_active,
    count(distinct s.customer_id) as unique_customers,
    concat_ws(', ', sort_array(collect_set(s.country))) as country_values
from sessions s
join customers c on s.customer_id = c.id
where s.device not in ('mobile', 'desktop', 'tablet')
group by year(c.signup_date), c.is_active
order by unique_customers desc
