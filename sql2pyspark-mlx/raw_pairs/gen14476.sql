select ss.store_id, case when ss.units >= 43 then 'high' when ss.units >= 9 then 'mid' else 'low' end as units_bucket, max(i.quantity) - min(i.quantity) as quantity_range
from inventory i
join store_sales ss on i.store_id = ss.store_id and i.product_id = ss.product_id
where ss.revenue >= 3900
group by ss.store_id, case when ss.units >= 43 then 'high' when ss.units >= 9 then 'mid' else 'low' end
order by quantity_range desc, ss.store_id desc
