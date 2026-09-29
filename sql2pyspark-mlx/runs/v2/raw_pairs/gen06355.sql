select customers.city, count(sessions.id) as session_count, count(distinct sessions.browser) as unique_browsers
from customers
left join sessions on sessions.customer_id = customers.id
group by customers.city;
