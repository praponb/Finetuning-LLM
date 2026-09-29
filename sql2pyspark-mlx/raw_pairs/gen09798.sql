select sqft, opened_date
from stores
where opened_date >= '2023-08-01'
union all
select sqft, opened_date
from stores
where region = 'North'
