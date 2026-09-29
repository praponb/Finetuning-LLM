select
    s.region,
    month(s.opened_date) as opened_month,
    max(i.quantity) - min(i.quantity) as quantity_range,
    count(i.quantity) as quantity_count
from inventory i
left outer join products p on p.id = i.product_id
full outer join stores s on s.id = i.store_id
group by s.region, month(s.opened_date)
order by quantity_count desc
