select c.country, count(distinct s.browser) as unique_browsers
from sessions s
inner join customers c on s.customer_id = c.id
where c.credit_limit <= 4500
group by c.country;
