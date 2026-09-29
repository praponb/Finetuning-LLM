select id, rating, case when country in ('UK', 'US') then 'Core' else 'Other' end as country_group
from suppliers
order by id desc, rating desc
