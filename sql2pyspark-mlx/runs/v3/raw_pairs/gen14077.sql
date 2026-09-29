select
    s.id,
    s.cost,
    o.status,
    st.city
from shipments s
inner join orders o on s.order_id = o.id
left outer join stores st on st.id = o.store_id
where s.carrier = 'FedEx'
