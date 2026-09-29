select
    s.id,
    s.region,
    s.opened_date,
    s.sqft
from stores s
where s.city in ('Berlin', 'Bangkok', 'London') and exists (select 1 from store_sales ss where ss.store_id = s.id and ss.revenue <= 2900)
order by sqft desc
