select
    s.city,
    ss.sale_date,
    ss.units,
    ss.store_id
from stores s
left outer join store_sales ss on s.id = ss.store_id
where ss.sale_date < '2024-02-01'
order by units desc;
