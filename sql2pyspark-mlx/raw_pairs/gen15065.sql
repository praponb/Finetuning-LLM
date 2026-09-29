select last_restock
from inventory
intersect
select opened_date
from accounts
order by last_restock;
