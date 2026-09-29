select
    ss.revenue,
    ss.store_id,
    p.product_name,
    p.cost,
    s.region
from store_sales ss
left join products p on p.id = ss.product_id
inner join stores s on s.id = ss.store_id
where s.region != 'South' or s.sqft != 8656
order by s.region
