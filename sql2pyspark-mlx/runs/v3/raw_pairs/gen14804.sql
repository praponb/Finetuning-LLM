select round((100 * sum(case when country = 'DE' then 1 else 0 end)) / count(*), 1) as pct_de
from customers
where email like '%@startup.io' and (is_active or segment != 'Corporate');
