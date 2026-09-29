select opened_date
from stores
where sqft >= 7038
union
select order_date
from orders
where shipping_fee between 25 and 35
order by opened_date desc;
