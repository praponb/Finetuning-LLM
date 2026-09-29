select st.region, sum(s.cost) as total_cost from shipments s right join orders o on o.id = s.order_id left join stores st on o.store_id = st.id group by st.region
