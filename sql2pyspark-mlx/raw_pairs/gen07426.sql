select unit_price as value
from order_items
intersect
select amount as value
from transactions
where txn_time >= '2023-08-01';
