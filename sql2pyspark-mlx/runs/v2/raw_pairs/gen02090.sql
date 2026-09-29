select id, opened_date
from stores
where region in ('Central', 'East')
union
select id, opened_date
from stores
where sqft between 4615 and 8002
order by id;
