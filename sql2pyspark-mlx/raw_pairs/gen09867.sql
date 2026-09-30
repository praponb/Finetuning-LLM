select year(c.signup_date) as signup_year, round(avg(o.discount), 2) as avg_discount, sum(o.discount) as total_discount
from orders o
left join stores s on s.id = o.store_id
inner join customers c on o.customer_id = c.id
group by year(c.signup_date)
order by signup_year desc;
