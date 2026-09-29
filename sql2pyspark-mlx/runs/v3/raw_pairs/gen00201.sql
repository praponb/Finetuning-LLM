select status
from accounts
where account_type <> 'LOAN'
except
select status
from payments
where method is not null
