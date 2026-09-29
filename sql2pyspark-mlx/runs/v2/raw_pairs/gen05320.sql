select status
from accounts
except
select status
from payments
where amount between 980 and 1250
order by status;
