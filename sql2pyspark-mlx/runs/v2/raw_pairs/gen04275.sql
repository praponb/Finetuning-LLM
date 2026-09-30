select status
from orders
union all
select status
from accounts
where opened_date > '2024-09-12'
order by status
