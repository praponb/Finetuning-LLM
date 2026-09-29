select id, store_name
from stores
where region not in ('Central', 'East', 'North');
