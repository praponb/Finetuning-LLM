select revenue as value
from store_sales
where revenue is not null
union all
select amount as value
from payments
where amount is not null
