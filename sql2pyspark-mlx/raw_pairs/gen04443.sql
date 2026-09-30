select shipping_fee, 'order' as source
from orders
union all
select amount, 'transaction' as source
from transactions
where merchant in ('Grab', 'Amazon', 'Starbucks')
