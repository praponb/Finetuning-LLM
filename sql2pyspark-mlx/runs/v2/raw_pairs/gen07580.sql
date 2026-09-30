select count(case when country = 'JP' then 1 end) as jp_count, sum(case when city = 'Chiang Mai' then rating else 0 end) as chiang_mai_rating
from suppliers
where length(supplier_name) > 8 and city <> 'Singapore'
order by jp_count desc, chiang_mai_rating;
