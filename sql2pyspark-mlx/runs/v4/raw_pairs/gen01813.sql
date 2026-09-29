select city, 'customer' as source
from customers
where signup_date <= '2024-06-02'
union
select city, 'store' as source
from stores
where region not in ('Central', 'East', 'North');
