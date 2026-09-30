select id, city
from stores
where store_name is not null
union
select id, city
from stores
where city in ('Berlin', 'Chiang Mai', 'Tokyo')
order by id desc
