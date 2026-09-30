select id, customer_id
from sessions
where device in ('mobile', 'tablet')
intersect
select id, customer_id
from sessions
where customer_id is not null;
