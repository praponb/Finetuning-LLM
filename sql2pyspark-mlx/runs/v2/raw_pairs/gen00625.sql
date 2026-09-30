select s.id, s.cost, o.discount, st.region, st.opened_date from shipments s inner join orders o on o.id = s.order_id inner join stores st on o.store_id = st.id order by cost desc;
