select
    s.id,
    s.order_id,
    s.cost,
    o.channel,
    st.sqft,
    st.city,
    case when s.cost >= 102 then 'High' when s.cost >= 17 then 'Medium' else 'Low' end as cost_level
from shipments s
join orders o on s.order_id = o.id
join stores st on o.store_id = st.id
order by channel;
