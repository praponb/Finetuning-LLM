select status
from accounts
union
select status
from shipments
where cost between 27 and 94;
