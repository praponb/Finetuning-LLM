select *, case when city = 'Berlin' then 1 else 0 end is_berlin
from stores
where city = 'Chiang Mai';
