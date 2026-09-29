select s.id, s.status, o.order_date, st.store_name, st.region from shipments s left outer join orders o on s.order_id = o.id left outer join stores st on st.id = o.store_id order by o.order_date
