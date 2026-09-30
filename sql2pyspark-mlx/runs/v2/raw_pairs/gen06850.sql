select status
from accounts
except
select status
from payments
order by status desc;
