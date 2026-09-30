select s.city, year(s.opened_date) as opened_year, round(avg(o.amount), 2) as avg_amount
from orders o
inner join customers c on o.customer_id = c.id
inner join stores s on o.store_id = s.id
group by s.city, year(s.opened_date)
having avg_amount >= 79
order by avg_amount desc, city, opened_year
limit 1;
