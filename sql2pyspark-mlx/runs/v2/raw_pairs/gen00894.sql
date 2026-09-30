select case when o.shipping_fee >= 41 then 'high' when o.shipping_fee >= 17 then 'mid' else 'low' end as shipping_fee_bucket, max(p.paid_at) as last_paid_at
from payments p
left outer join orders o on p.order_id = o.id
group by case when o.shipping_fee >= 41 then 'high' when o.shipping_fee >= 17 then 'mid' else 'low' end
order by last_paid_at asc, shipping_fee_bucket asc;
