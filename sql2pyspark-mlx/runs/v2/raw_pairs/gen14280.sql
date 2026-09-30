select count(case when country = 'DE' then 1 end) de_count, min(age) min_age
from customers
