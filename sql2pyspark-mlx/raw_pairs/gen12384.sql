select st.region, max(s.cost) - min(s.cost) as cost_range, min(s.cost) as min_cost
from shipments s
left outer join orders o on o.id = s.order_id
left outer join stores st on st.id = o.store_id
where o.store_id is null
group by st.region;
