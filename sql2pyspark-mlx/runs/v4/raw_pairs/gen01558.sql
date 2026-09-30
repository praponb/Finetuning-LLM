select
    s.id,
    s.delivered_date,
    s.order_id,
    o.customer_id,
    st.sqft
from shipments s
left join orders o on s.order_id = o.id
join stores st on o.store_id = st.id
where month(s.delivered_date) in (5, 8, 12) and s.carrier in ('FedEx', 'Flash', 'DHL');
