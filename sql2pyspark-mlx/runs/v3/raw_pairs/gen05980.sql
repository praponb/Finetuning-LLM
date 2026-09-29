select s.country, c.is_active, c.name
from sessions s
right join customers c on s.customer_id = c.id
where c.country != 'US'
order by c.name, country
