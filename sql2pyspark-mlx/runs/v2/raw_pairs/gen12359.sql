select status, 'account' source
from accounts
union all
select status, 'payment' source
from payments
