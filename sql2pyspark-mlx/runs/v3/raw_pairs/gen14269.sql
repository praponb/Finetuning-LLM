select
    s.id,
    s.carrier,
    o.channel,
    st.opened_date
from shipments s
left join orders o on o.id = s.order_id
inner join stores st on o.store_id = st.id
where o.store_id is null
order by s.carrier, id
limit 3;
