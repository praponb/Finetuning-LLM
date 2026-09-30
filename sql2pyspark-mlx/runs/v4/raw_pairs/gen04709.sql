select year(c.signup_date) as signup_year, c.city, sort_array(collect_set(s.country)) as country_list
from sessions s
join customers c on s.customer_id = c.id
group by year(c.signup_date), c.city
