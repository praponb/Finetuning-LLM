select s.id, s.carrier, s.delivered_date, o.amount, o.shipping_fee, st.city, st.region from shipments s inner join orders o on s.order_id = o.id left join stores st on o.store_id = st.id;
