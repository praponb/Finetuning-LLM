select region
from stores
where sqft = 15001
union all
select region
from stores
where opened_date between '2024-01-30' and '2024-07-30';
