select
    s.id,
    s.carrier,
    o.discount,
    o.customer_id,
    st.region,
    st.store_name
from shipments s
left outer join orders o on o.id = s.order_id
right outer join stores st on o.store_id = st.id
where month(s.shipped_date) in (3, 8, 9)
order by s.id desc
