select opened_date
from stores
except
select end_date as opened_date
from subscriptions;
