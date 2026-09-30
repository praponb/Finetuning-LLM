select status
from orders
where discount >= 0.28
union all
select status
from accounts
where opened_date >= '2023-06-01';
