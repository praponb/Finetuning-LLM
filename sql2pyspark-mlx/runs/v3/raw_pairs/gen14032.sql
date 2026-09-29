select opened_date, status
from accounts
where balance <= 56000
union all
select opened_date, status
from accounts
where balance < 45000;
