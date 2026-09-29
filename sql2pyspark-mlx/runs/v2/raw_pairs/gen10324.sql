select region
from stores
where opened_date >= '2024-02-05'
union
select region
from stores
where city = 'Singapore';
