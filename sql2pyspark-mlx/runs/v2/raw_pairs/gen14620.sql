select
    ss.id,
    ss.product_id,
    ss.store_id,
    p.launch_date,
    p.price,
    s.sqft,
    s.store_name,
    case when ss.units >= 44 then 'High' when ss.units >= 19 then 'Medium' else 'Low' end as units_band
from store_sales ss
left join products p on ss.product_id = p.id
left join stores s on ss.store_id = s.id
order by 8 desc;
