select o.channel, case when o.discount >= 0.33 then 'high' when o.discount >= 0.14 then 'mid' else 'low' end as discount_bucket, count(*) as row_count
from payments p
left join orders o on p.order_id = o.id
left join stores s on o.store_id = s.id
group by o.channel, case when o.discount >= 0.33 then 'high' when o.discount >= 0.14 then 'mid' else 'low' end;
