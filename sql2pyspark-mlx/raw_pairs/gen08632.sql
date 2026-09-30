select region, opened_date
from stores
where store_name is not null
union
select region, opened_date
from stores
where store_name like 'Mal%'
