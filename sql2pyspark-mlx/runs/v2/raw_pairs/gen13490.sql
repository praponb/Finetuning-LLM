select s.id, s.carrier, o.channel, st.opened_date from shipments s left join orders o on s.order_id = o.id inner join stores st on o.store_id = st.id order by id desc
