select st.city, st.region, count(distinct s.cost) as unique_statuss from shipments s left join orders o on s.order_id = o.id join stores st on o.store_id = st.id group by st.city, st.region
