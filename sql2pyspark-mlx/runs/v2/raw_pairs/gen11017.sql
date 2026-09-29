select country
from sessions
where started_at < '2025-09-01'
union
select country
from sessions
where customer_id is null
order by country;
