select opened_date
from accounts
where opened_date is not null
union all
select last_restock
from inventory
where last_restock is not null;
