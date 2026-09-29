select region as label
from stores
except
select status as label
from payments
where method <> 'paypal'
order by label;
