select
    s.opened_date,
    s.region,
    s.city,
    ss.revenue
from stores s
left outer join store_sales ss on s.id = ss.store_id
where s.region <> 'South'
